$ErrorActionPreference = "Stop"

python -m pip install -r requirements.txt -r requirements-dev.txt
pyinstaller --noconsole --onefile --name FacturadorGUI --paths src --add-data "config\\reglas_especiales.xlsx;config" --add-data "config\\update_config.json;config" run_gui.py

Write-Output "Exe generado en dist\\FacturadorGUI.exe"
