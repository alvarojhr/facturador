param(
    [Parameter(Mandatory = $true)]
    [string]$InstallerPath,
    [Parameter(Mandatory = $true)]
    [string]$Version,
    [Parameter(Mandatory = $true)]
    [string]$InstallerUrl,
    [string]$Notes = "",
    [bool]$Mandatory = $false,
    [string]$OutputPath = "update_manifest.json"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $InstallerPath)) {
    throw "No existe el instalador: $InstallerPath"
}

$hash = (Get-FileHash -Path $InstallerPath -Algorithm SHA256).Hash.ToLower()
$manifest = @{
    version = $Version
    installer_url = $InstallerUrl
    sha256 = $hash
    mandatory = $Mandatory
    notes = $Notes
}

$json = $manifest | ConvertTo-Json -Depth 4
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$output = Join-Path (Get-Location) $OutputPath
$outputDir = Split-Path -Parent $output
if ($outputDir -and -not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}
[System.IO.File]::WriteAllText($output, "$json`n", $utf8NoBom)
Write-Output "Manifest generado en $output"
