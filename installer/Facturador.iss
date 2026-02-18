[Setup]
AppName=Facturador
AppVersion=0.3.0
DefaultDirName={commonpf}\Facturador
DefaultGroupName=Facturador
OutputDir=dist-installer
OutputBaseFilename=FacturadorSetup
Compression=lzma
SolidCompression=yes
AppId={{6CDE64B6-0F59-4C91-97E7-00EBAF59B6A2}

[Tasks]
Name: "desktopicon"; Description: "Crear icono en el escritorio"; Flags: unchecked

[Files]
Source: "..\\config\\reglas_especiales.xlsx"; DestDir: {app}\\config; Flags: ignoreversion
Source: "..\\config\\update_config.json"; DestDir: {app}\\config; Flags: ignoreversion
Source: "..\\config\\mail_automation.json"; DestDir: {app}\\config; Flags: ignoreversion
Source: "..\\config\\mail_automation.example.json"; DestDir: {app}\\config; Flags: ignoreversion
Source: "..\dist\FacturadorGUI.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\dist\FacturadorMailAutomation.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Facturador"; Filename: "{app}\FacturadorGUI.exe"
Name: "{group}\Facturador - Automatizacion Gmail"; Filename: "{app}\FacturadorMailAutomation.exe"
Name: "{commondesktop}\Facturador"; Filename: "{app}\FacturadorGUI.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\FacturadorGUI.exe"; Description: "Abrir Facturador"; Flags: nowait postinstall skipifsilent

