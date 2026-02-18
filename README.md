# Facturador a Excel

Script en Python que toma un XML de factura (AttachedDocument con una Invoice DIAN) y genera un archivo Excel con los precios de costo y de venta por producto.

## Requisitos
- Python 3.10+ (probado en Windows)
- `pip install -r requirements.txt`

## Uso rapido
1) Crear entorno y dependencias:
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
2) Coloca tus XML o ZIP en la carpeta `invoices/` (se crea sola al correr el script) para mantener orden.

3) Ejecutar el conversor:
```bash
python run.py --input fv08002414950152500163418.xml
python run.py --input factura.zip
```
4) Ejecutar la interfaz grafica:
```bash
python run_gui.py
```
5) Ejecutar automatizacion de Gmail -> Drive:
```bash
python run_mail_automation.py --once
python run_mail_automation.py
```
6) Configurar actualizaciones automaticas:
- El proyecto ya apunta a `https://facturador.ferreteriapinki.com/update_manifest.json`.
- Si necesitas otro endpoint, edita `config/update_config.json`.
Parametros utiles:
- `--markup-threshold 10000` umbral de costo neto para decidir la utilidad.
- `--markup-below 0.68` divisor cuando el costo neto es menor al umbral.
- `--markup-above 1.32` multiplicador cuando el costo neto es mayor o igual al umbral.
- `--round-net-step 100` paso de redondeo para la venta neta (por defecto centena).
- `--rounding-mode nearest|up|down` estrategia de redondeo (por defecto `up`, evita cerrar en miles exactos).
- `--rules ruta.xlsx` reglas especiales de utilidad (opcional). Por defecto usa `config/reglas_especiales.xlsx` si existe.

## Que hace
- Busca dentro del AttachedDocument el XML de `<Invoice>` (viene en `cbc:Description` como CDATA). Si el ZIP trae un XML de `Invoice` directo, lo usa tambien.
- Lee cada `cac:InvoiceLine` y calcula:
  - `Descuento %`: a partir de `BaseAmount` y `LineExtensionAmount` o de los `AllowanceCharge` de la linea.
  - `Costo bruto unitario`: `BaseAmount / Cantidad` (sin descuento, antes de IVA).
  - `Costo neto unitario`: aplica descuento y luego IVA al costo bruto.
  - `Venta neto unitario`: sobre el costo neto aplica utilidad (divide en `markup-below` si es menor al umbral, si no multiplica por `markup-above`); luego redondea al paso indicado, por defecto hacia arriba y si cae en multiplo de mil, sube una centena extra.
  - `Venta bruta unitario`: en el Excel es una formula que deriva del neto (`=venta_neta/(1+IVA)`), para que cambios en el neto se reflejen.
- Genera `salida.xlsx` con columnas (hoja `Productos`): Linea factura, Producto, Cantidad, IVA %, Costo bruto, Costo neto (formula), Venta bruta (formula), Venta neta (formula), Valor total Neto compra (formula) y Descuento %.
- Las columnas E, F, G y H quedan enlazadas por formulas: si editas `Costo bruto unitario` (E), se recalculan costo neto, venta bruta y venta neta.
- Agrega una hoja `Encabezado` con datos del proveedor, factura, CUFE, fechas y totales.

## Notas
- Los calculos usan `Decimal` y se redondean a 2 decimales al escribir el Excel.
- Si hay mas de un documento embebido, se toma el primero que contenga `<Invoice`.
- El codigo no requiere conexion a red ni modifica el XML original.
- Si pasas `--input nombre.xml` y no existe en ruta absoluta, el script lo buscara en `invoices/nombre.xml`.
- Si pasas un ZIP, el script tomara el primer XML valido dentro del archivo, generara una carpeta con el numero de factura y guardara el PDF y el XLSX dentro.
- Si indicas `--output` con un ZIP, debe ser una carpeta base donde se creara la carpeta de la factura.
- Puedes definir reglas especiales de utilidad en `config/reglas_especiales.xlsx` (columnas: `match_type`, `pattern`, `utilidad_percent`). Ejemplo: `contains | VARILLA | 12`.
- Si el XLSX de una factura ya existe, no se sobrescribe; se conserva el archivo editado.
- En la GUI, despues de procesar se abre automaticamente la carpeta de salida de la factura.
- En la GUI, la app busca actualizaciones al iniciar (si `manifest_url` esta configurado) y tambien desde el boton `Buscar actualizaciones`.

## Automatizacion Gmail -> Google Drive
La automatizacion hace polling a Gmail, busca correos con adjuntos `.zip`, procesa cada ZIP con Facturador y sube la carpeta resultado (PDF + XLSX) a Google Drive.

### 1) Preparar credenciales OAuth de Google
1) En Google Cloud Console, crea un proyecto.
2) Habilita las APIs:
- Gmail API
- Google Drive API
3) Configura `OAuth consent screen` (tipo External o Internal segun tu caso).
4) Crea credencial `OAuth client ID` de tipo `Desktop app`.
5) Descarga el JSON y guardalo en:
`config/google_credentials.json`

### 2) Configurar carpeta destino de Drive
1) Crea en Drive una carpeta para facturas procesadas.
2) Abre la carpeta y copia el ID desde la URL:
`https://drive.google.com/drive/folders/<ID_AQUI>`
3) Edita `config/mail_automation.json` y pega el ID en:
`drive_parent_folder_id`

### 3) Configurar automatizacion
Usa `config/mail_automation.json` (incluido en el repo). Si quieres un ejemplo limpio, revisa `config/mail_automation.example.json`.

Campos clave:
- `gmail_query`: filtro de Gmail para buscar ZIPs.
- `processed_label_name`: etiqueta que se aplica para no reprocesar el correo.
- `poll_interval_sec`: intervalo de consulta.
- `drive_parent_folder_id`: carpeta destino en Drive.

### 4) Primera ejecucion (autorizacion)
```bash
python run_mail_automation.py --once --verbose
```
En la primera ejecucion se abrira el navegador para autorizar acceso a Gmail/Drive.
Se guarda token en `config/google_token.json`.

### 5) Ejecucion continua
```bash
python run_mail_automation.py --verbose
```
Para detener: `Ctrl+C`.

### 6) Produccion en Windows (recomendado)
Puedes correrlo con Python o con el ejecutable instalado (`FacturadorMailAutomation.exe`).

Ejemplo con `schtasks` (al iniciar sesion):
```bash
schtasks /Create /F /SC ONLOGON /TN "FacturadorMailAutomation" /TR "\"C:\Program Files\Facturador\FacturadorMailAutomation.exe\" --log-file \"C:\ProgramData\Facturador\mail_automation.log\""
```

Eliminar tarea:
```bash
schtasks /Delete /TN "FacturadorMailAutomation" /F
```

## Trigger en nube (Arquitectura 2)
Esta opcion usa Gmail Push Notifications (`watch`) + Pub/Sub + Cloud Run + Firestore para reaccionar a correos nuevos.

Componentes:
- Cloud Run service: endpoint `/pubsub/push` y endpoints admin (`/admin/start-watch`, `/admin/full-sync`).
- Pub/Sub topic/subscription push: recibe notificaciones de Gmail.
- Cloud Scheduler: renueva `watch` periodicamente y ejecuta sync de respaldo.
- Firestore: almacena `last_history_id`.

### Archivos agregados para esta arquitectura
- `src/facturador/mail_trigger_service.py`
- `run_mail_trigger_service.py`
- `main.py`
- `deploy_gcp_gmail_trigger.ps1`

### Despliegue productivo automatizado
1) Instala y autentica Google Cloud SDK:
```bash
gcloud auth login
```
2) Asegura que existan estos archivos locales:
- `config/mail_automation.json` (con `drive_parent_folder_id` listo)
- `config/google_credentials.json`
- `config/google_token.json`

3) Ejecuta el script de despliegue:
```bash
powershell -ExecutionPolicy Bypass -File .\deploy_gcp_gmail_trigger.ps1 -ProjectId TU_PROJECT_ID
```

El script realiza:
- habilitar APIs necesarias,
- crear service accounts,
- crear/actualizar secretos,
- desplegar Cloud Run,
- crear topic/subscription de Pub/Sub,
- configurar jobs de Scheduler,
- ejecutar `start-watch` inicial.

### Variables utiles del script
- `-Region` (default `us-central1`)
- `-WatchLabelIds` (default `INBOX`)
- `-WatchSchedule` (default cada 6 horas)
- `-FullSyncSchedule` (default cada 15 minutos)

### Nota para Gmail personal
- Se usa OAuth de usuario (no service account Gmail).
- El `google_token.json` se genera primero en local y luego se sube como secreto para Cloud Run.

## Repositorio Git (local)
Inicializa el repo local (si aun no existe) y haz el primer commit:
```bash
git init
git add .
git commit -m "feat: base Facturador con updater"
```
El proyecto incluye `.gitignore` para no subir artefactos locales (`dist`, `build`, `invoices` de pruebas, etc.).

## Publicacion con GitHub Releases + GitHub Pages
Estrategia:
- `FacturadorSetup.exe` se publica en **GitHub Releases**.
- `update_manifest.json` se publica en **GitHub Pages**.
- La app consulta `manifest_url` y descarga el instalador desde Releases.

### 1) Crear repositorio remoto en GitHub
1) Crea un repositorio vacio (ej: `Facturador`).
2) Conecta el remoto y sube la rama principal:
```bash
git remote add origin https://github.com/TU_USUARIO/Facturador.git
git branch -M main
git push -u origin main
```

### 2) Habilitar GitHub Pages
1) En GitHub: `Settings > Pages`.
2) En `Build and deployment`, selecciona `GitHub Actions`.

### 3) Configurar subdominio `facturador.ferreteriapinki.com`
1) En `Settings > Pages > Custom domain`, define:
`facturador.ferreteriapinki.com`
2) En el DNS de `ferreteriapinki.com`, crea:
- Tipo: `CNAME`
- Host/Name: `facturador`
- Target/Value: `TU_USUARIO.github.io`

### 4) Publicar una version nueva
El workflow incluido `.github/workflows/release-and-pages.yml` hace todo:
- compila EXE + instalador en Windows,
- crea Release con `FacturadorSetup.exe`,
- genera `update_manifest.json`,
- publica el manifest en GitHub Pages.

Ejecutalo desde: `Actions > Release And Publish Manifest > Run workflow`
Inputs:
- `version`: por ejemplo `0.3.1`
- `notes`: notas de version (opcional)
- `mandatory`: `true/false`
- `publish_manifest`: `true` (recomendado)

### 5) Configuracion del instalador en clientes
`config/update_config.json` debe tener:
```json
{
  "manifest_url": "https://facturador.ferreteriapinki.com/update_manifest.json"
}
```

## Instalador (Windows, build local manual)
Si quieres construir localmente sin Actions:
1) Instalar dependencias de build:
```bash
pip install -r requirements.txt -r requirements-dev.txt
```
2) Generar el exe:
```bash
./build_exe.ps1
```
3) Compilar instalador:
```bash
# usar Inno Setup
```
4) Generar manifest manual:
```bash
./build_update_manifest.ps1 -InstallerPath .\installer\dist-installer\FacturadorSetup.exe -Version 0.3.0 -InstallerUrl https://github.com/TU_USUARIO/Facturador/releases/download/v0.3.0/FacturadorSetup.exe -OutputPath update_manifest.json
```
