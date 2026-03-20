# Runbook: Facturador -> ERP Local Con Gmail Real

## Objetivo
Probar el flujo completo de Facturador contra el ERP local, dejando las compras en la BD local de `FerreteriaPinki`.

## 1. Levantar Pinki local
Desde `C:\Utilities\FerreteriaPinki`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-local-facturador-test.ps1
```

Resultado esperado:
- Frontend en `http://localhost:5173`
- Backend en `http://localhost:3000`
- DIAN en `http://localhost:8000`

## 2. Preparar Facturador para ERP local
Desde `C:\Utilities\Facturador`:

```powershell
powershell -ExecutionPolicy Bypass -File .\prepare_local_erp_test.ps1
```

Ese script:
- toma `config/mail_automation.json` como base;
- crea o reutiliza una carpeta `QA Local ERP` en Drive, debajo de la carpeta raiz ya configurada;
- crea `config/local/mail_automation.local-erp.json`;
- apunta `erp_base_url` a `http://localhost:3000`;
- usa `erp_api_key=dev-ingest-key-2026`.

## 3. Reprocesar correos reales
En Gmail:
- quita `facturador-procesado` a 1-3 correos;
- verifica que sigan en `INBOX`.

## 4. Ejecutar un ciclo local
Desde `C:\Utilities\Facturador`:

```powershell
python run_mail_automation.py --config config/local/mail_automation.local-erp.json --once --verbose
```

Por defecto, esta ejecucion ahora procesa sin limite de mensajes. Si quieres acotarla:

```powershell
python run_mail_automation.py --config config/local/mail_automation.local-erp.json --once --verbose --max-messages-per-poll 25
```

## 5. Validar
- UI de Compras en `http://localhost:5173`
- logs de Facturador con `ERP ingestion OK`
- tablas `purchase_invoices` y `purchase_invoice_lines` en la BD local

## 6. Repetir la prueba
Opciones:
- quitar otra vez `facturador-procesado` a otros correos;
- limpiar compras en la BD local;
- recrear la BD local y volver a sembrar.

## Notas
- Este flujo toca Gmail y Drive reales.
- La salida en Drive queda aislada dentro de la carpeta `QA Local ERP`.
- La ingesta al ERP es idempotente por proveedor + numero de factura.
