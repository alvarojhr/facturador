$ErrorActionPreference = "Stop"

python -m pip install -r requirements.txt -r requirements-dev.txt
pyinstaller --noconsole --onefile --name FacturadorGUI --paths src --add-data "config\\reglas_especiales.xlsx;config" --add-data "config\\update_config.json;config" --add-data "config\\mail_automation.json;config" --add-data "config\\mail_automation.example.json;config" run_gui.py
pyinstaller --onefile --name FacturadorMailAutomation --paths src --add-data "config\\reglas_especiales.xlsx;config" --add-data "config\\mail_automation.json;config" --add-data "config\\mail_automation.example.json;config" run_mail_automation.py

Write-Output "Exes generados en dist\\FacturadorGUI.exe y dist\\FacturadorMailAutomation.exe"
