param(
    [string]$SourceConfigPath = "config/mail_automation.json",
    [string]$OutputConfigPath = "config/local/mail_automation.local-erp.json",
    [string]$LocalErpBaseUrl = "http://localhost:3000",
    [string]$LocalErpApiKey = "dev-ingest-key-2026",
    [string]$QaFolderName = "QA Local ERP"
)

$ErrorActionPreference = "Stop"

function Resolve-RepoPath([string]$PathValue) {
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot $PathValue))
}

function Assert-Tool([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "No se encontro '$Name' en PATH."
    }
}

Assert-Tool python

$sourceConfigFullPath = Resolve-RepoPath $SourceConfigPath
$outputConfigFullPath = Resolve-RepoPath $OutputConfigPath

if (-not (Test-Path $sourceConfigFullPath)) {
    throw "No existe config de Facturador: $sourceConfigFullPath"
}

$outputDir = Split-Path -Parent $outputConfigFullPath
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$pythonPath = Join-Path $PSScriptRoot "src"
$previousPythonPath = $env:PYTHONPATH
$env:PYTHONPATH = if ($previousPythonPath) { "$pythonPath;$previousPythonPath" } else { $pythonPath }

try {
    $driveInfoJson = @'
import json
import sys
from pathlib import Path

from facturador.mail_automation import load_mail_automation_config, MailAutomationService

config_path = Path(sys.argv[1])
qa_folder_name = sys.argv[2]

cfg = load_mail_automation_config(config_path)
svc = MailAutomationService(cfg)
qa_folder_id = svc._ensure_drive_folder(qa_folder_name, cfg.drive_parent_folder_id)
ingresado_folder_id = svc._ensure_drive_folder(cfg.entered_drive_subfolder_name, qa_folder_id)

print(json.dumps({
    "source_drive_parent_folder_id": cfg.drive_parent_folder_id,
    "qa_folder_name": qa_folder_name,
    "qa_folder_id": qa_folder_id,
    "ingresado_folder_id": ingresado_folder_id,
    "entered_drive_subfolder_name": cfg.entered_drive_subfolder_name,
}, ensure_ascii=True))
'@ | python - $sourceConfigFullPath $QaFolderName
} finally {
    $env:PYTHONPATH = $previousPythonPath
}

if (-not $driveInfoJson) {
    throw "No se pudo obtener la carpeta QA de Drive."
}

$driveInfo = $driveInfoJson | ConvertFrom-Json
$config = Get-Content $sourceConfigFullPath -Raw | ConvertFrom-Json
$config.erp_base_url = $LocalErpBaseUrl
$config.erp_api_key = $LocalErpApiKey
$config.drive_parent_folder_id = $driveInfo.qa_folder_id
$config.local_work_dir = "automation_work/local-erp"

$configJson = $config | ConvertTo-Json -Depth 20
[System.IO.File]::WriteAllText(
    $outputConfigFullPath,
    "$configJson`n",
    (New-Object System.Text.UTF8Encoding($false))
)

Write-Output "Configuracion local lista."
Write-Output "Config origen: $sourceConfigFullPath"
Write-Output "Config local:  $outputConfigFullPath"
Write-Output "QA folder:     $($driveInfo.qa_folder_name)"
Write-Output "QA folder id:  $($driveInfo.qa_folder_id)"
Write-Output "ERP local:     $LocalErpBaseUrl"
Write-Output "API key:       $LocalErpApiKey"
Write-Output "Siguiente comando:"
Write-Output "python run_mail_automation.py --config $outputConfigFullPath --once --verbose"
