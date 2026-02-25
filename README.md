# Facturador (Automatizacion Gmail -> Google Drive)

Facturador procesa facturas que llegan por correo (ZIP con XML + PDF), genera un XLSX con las reglas de negocio y sube el resultado a Google Drive.

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
- `drive_parent_folder_id`: carpeta raiz en Drive donde se crean carpetas por factura.
- `entered_label_name`: label de negocio que indica que la compra ya fue ingresada en ERP (`Ingresado`).
- `entered_synced_label_name`: label tecnico para marcar que el movimiento en Drive ya fue aplicado.
- `entered_drive_subfolder_name`: subcarpeta destino dentro de `drive_parent_folder_id` (por defecto `Ingresado`).
- `sync_entered_label`: activa/desactiva esta sincronizacion.

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

Ejecucion continua:
```bash
python run_mail_automation.py --verbose
```

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

Si el refresh token OAuth expira o es revocado (`invalid_grant`):
- `GET /healthz` responde `200` con `{"ok": true, "automation_ready": false, "reason": "oauth_invalid_grant"}`.
- `POST /admin/start-watch` responde `503` con `code=oauth_unavailable`.
- `POST /admin/full-sync` responde `503` con `code=oauth_unavailable`.
- `POST /pubsub/push` responde `200` con `degraded=true` para evitar tormenta de reintentos.

Runbook operativo:
- [oauth_recovery_runbook.md](docs/oauth_recovery_runbook.md)

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

### Secrets requeridos en GitHub
En `Settings > Secrets and variables > Actions`, crea:
- `GCP_WIF_PROVIDER`
- `GCP_DEPLOY_SERVICE_ACCOUNT`

Valores configurados para este proyecto:
- `GCP_WIF_PROVIDER=projects/810301469982/locations/global/workloadIdentityPools/github-pool/providers/github-provider`
- `GCP_DEPLOY_SERVICE_ACCOUNT=facturador-github-deploy-sa@project-98c83c2b-f615-4eb7-93e.iam.gserviceaccount.com`

### Variables opcionales de Actions
Puedes sobreescribir defaults con variables del repo:
- `GCP_PROJECT_ID`
- `GCP_REGION`
- `GCP_SERVICE`
- `GCP_ARTIFACT_REPOSITORY`
- `GCP_IMAGE_NAME`
