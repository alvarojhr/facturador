# Facturador (Automatizacion Gmail -> ERP / Google Drive)

Facturador procesa facturas que llegan por correo (ZIP con XML + PDF), calcula precios, las ingiere en el ERP y opcionalmente genera/sincroniza artefactos en Google Drive.

Arquitectura productiva recomendada:
- Gmail Watch (push)
- Pub/Sub
- Cloud Run (`mail_trigger_service`)
- Cloud Scheduler (renovacion watch + full sync)
- Firestore (estado `historyId`)

## Requisitos
- Python 3.10+
- `pip install -r requirements.txt`

## Estructura principal
- `run_mail_automation.py`: ejecucion local por polling.
- `run_mail_trigger_service.py`: servidor Flask local para pruebas.
- `main.py`: entrypoint para Cloud Run (gunicorn).
- `src/facturador/mail_automation.py`: integracion Gmail/Drive + logica de procesamiento.
- `src/facturador/mail_trigger_service.py`: endpoints push/admin para nube.
- `deploy_gcp_gmail_trigger.ps1`: despliegue base en GCP.

## Configuracion
Archivo: `config/mail_automation.json`

Campos clave:
- `gmail_query`: filtro para correos con ZIP a procesar.
- `processed_label_name`: label tecnico para no reprocesar.
- `drive_parent_folder_id`: carpeta raiz en Drive donde se crean carpetas por factura. Requerido solo si la ejecucion usa Drive.
- `entered_label_name`: label de negocio que indica que la compra ya fue ingresada en ERP (`Ingresado`).
- `entered_synced_label_name`: label tecnico para marcar que el movimiento en Drive ya fue aplicado.
- `entered_drive_subfolder_name`: subcarpeta destino dentro de `drive_parent_folder_id` (por defecto `Ingresado`).
- `sync_entered_label`: activa/desactiva esta sincronizacion.
- `erp_base_url`: endpoint base del ERP para `POST /api/purchases/ingest`.
- `erp_api_key`: API key para ingesta desde Facturador.
- `artifacts_bucket_name`: bucket GCS opcional para publicar PDF/XML y enviarlos por referencia `gs://` al ERP.
- `artifacts_prefix`: prefijo dentro del bucket para organizar artefactos por factura.
- `token_store_project` / `token_store_collection` / `token_store_doc`: store opcional para persistir el refresh token OAuth en Firestore. Recomendado en Cloud Run para no depender de un mount de secretos de solo lectura.

Tambien puedes usar `config/mail_automation.example.json` como plantilla.

## Nuevo comportamiento: label `Ingresado`
Cuando un correo ya procesado tiene el label `Ingresado`:
1. Facturador identifica la factura asociada desde el ZIP del correo.
2. Busca en Drive la carpeta de esa factura (bajo la carpeta raiz configurada).
3. La mueve al subfolder `Ingresado`.
4. Marca el correo con `facturador-drive-ingresado` (o el label configurado) para no repetir.

Esto funciona tanto en:
- polling local (`run_mail_automation.py`)
- flujo push en Cloud Run (`/pubsub/push`)

## Ejecucion local (polling)
Primera autorizacion OAuth (una vez):
```bash
python run_mail_automation.py --once --verbose
```

Full dump al ERP sin usar Drive:
```bash
python run_mail_automation.py --once --skip-drive --concurrency 4 --verbose
```

Ejecucion continua:
```bash
python run_mail_automation.py --verbose
```

Overrides utiles:
- `--skip-drive`: omite Drive y procesa directo al ERP.
- `--skip-ingresado-sync`: omite la sincronizacion del label `Ingresado`.
- `--concurrency N`: workers de procesamiento para `--once`.
- `--max-messages-per-poll N`: limita el batch; si se omite, drena sin limite.

## Servicio local (modo trigger)
```bash
python run_mail_trigger_service.py
```

Healthcheck:
- `GET http://localhost:8080/healthz`

## Despliegue en GCP
Script recomendado:
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy_gcp_gmail_trigger.ps1 -ProjectId TU_PROJECT_ID
```

Asegura antes:
- `config/mail_automation.json`
- `config/google_credentials.json`
- `config/google_token.json`

## Notas
- El sistema procesa ZIPs con XML de tipo `Invoice`.
- Documentos no soportados (ej: credit notes) se omiten sin romper el ciclo.
- El XLSX conserva formulas entre columnas para recalculo manual.
- En produccion, `facturador-full-sync` esta configurado 1 vez al dia (`0 2 * * *`, `America/Bogota`).

## Resiliencia OAuth (Cloud Run)

Para eliminar la rotacion semanal del refresh token no basta con cambiar el almacenamiento:
- el OAuth consent screen debe estar en estado `In production`;
- si el proyecto es `External` y queda en `Testing`, Google emite refresh tokens que expiran en 7 dias para scopes como Gmail/Drive.

Persistencia recomendada en nube:
- `config/google_token.json` en Secret Manager solo como bootstrap inicial;
- Firestore como store writable del refresh token vigente durante los refresh automáticos.

Si el refresh token OAuth expira o es revocado (`invalid_grant`):
- `GET /healthz` responde `200` con `{"ok": true, "automation_ready": false, "reason": "oauth_invalid_grant"}`.
- `POST /admin/start-watch` responde `503` con `code=oauth_unavailable`.
- `POST /admin/full-sync` responde `503` con `code=oauth_unavailable`.
- `POST /pubsub/push` responde `200` con `degraded=true` para evitar tormenta de reintentos.

`POST /admin/full-sync` ahora acepta estos query params opcionales:
- `max_cycles`
- `max_messages_per_poll`
- `skip_drive=1`
- `skip_ingresado_sync=1`
- `concurrency=4`

Runbook operativo:
- [oauth_recovery_runbook.md](docs/oauth_recovery_runbook.md)

Rotacion asistida del token OAuth:
```powershell
powershell -ExecutionPolicy Bypass -File .\rotate_google_oauth_token.ps1 -ProjectId TU_PROJECT_ID
```
El script refresca o reautentica el token, publica una nueva version en Secret Manager, corrige `FACTURADOR_WATCH_TOPIC` usando el `project_id` de `config/google_credentials.json` y valida `watch-renew` y `full-sync` via Cloud Scheduler.

Cuando `FACTURADOR_TOKEN_STATE_COLLECTION` esta configurado, el servicio tambien copia el token valido a Firestore y deja de depender del mount de `/secrets/token/google_token.json` para los refresh posteriores.

Configurar metricas y alertas:
```powershell
powershell -ExecutionPolicy Bypass -File .\configure_monitoring_alerts.ps1 -ProjectId TU_PROJECT_ID
```

## CI/CD (GitHub Actions -> Cloud Run)
Se agrego el workflow:
- `.github/workflows/cloud-run-cicd.yml`

Comportamiento:
- `pull_request` a `main`: valida dependencias y compilacion.
- `push` a `main`: valida + build de imagen + deploy a Cloud Run.
- `workflow_dispatch`: deploy manual bajo demanda.

### Variables requeridas de Actions
En `Settings > Secrets and variables > Actions`, configura estas variables del repo:
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_SERVICE`
- `GCP_ARTIFACT_REPOSITORY`
- `GCP_IMAGE_NAME`
- `GCP_WIF_PROVIDER`
- `GCP_DEPLOY_SERVICE_ACCOUNT`

Valores esperados para el entorno preprod:
- `GCP_PROJECT_ID=facturador-preprod`
- `GCP_REGION=us-central1`
- `GCP_SERVICE=facturador-gmail-trigger`
