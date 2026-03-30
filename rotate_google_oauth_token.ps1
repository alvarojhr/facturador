param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,
    [string]$Region = "us-central1",
    [string]$ServiceName = "facturador-gmail-trigger",
    [string]$SchedulerLocation = "us-central1",
    [string]$WatchRenewJobName = "facturador-watch-renew",
    [string]$FullSyncJobName = "facturador-full-sync",
    [string]$TopicName = "facturador-gmail-updates",
    [string]$CredentialsPath = "config/google_credentials.json",
    [string]$TokenPath = "config/google_token.json",
    [string]$TokenSecretName = "facturador-google-token",
    [string]$TokenStateCollection = "facturador_state",
    [string]$TokenStateDoc = "gmail_oauth_token",
    [switch]$SkipFullSync
)

$ErrorActionPreference = "Stop"

$defaultGcloudBin = Join-Path $env:LOCALAPPDATA "Google\Cloud SDK\google-cloud-sdk\bin"
$defaultGcloudCmd = Join-Path $defaultGcloudBin "gcloud.cmd"
if (Test-Path $defaultGcloudCmd) {
    $env:PATH = "$defaultGcloudBin;$env:PATH"
}
$gcloudExe = if (Test-Path $defaultGcloudCmd) { $defaultGcloudCmd } else { "gcloud.cmd" }

function Assert-Tool([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "No se encontro '$Name' en PATH."
    }
}

function Invoke-GcloudCapture([string[]]$CommandArgs) {
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        return & $gcloudExe @CommandArgs 2>&1
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
}

function Get-OAuthProjectId([string]$CredentialsFilePath) {
    $oauthConfig = Get-Content $CredentialsFilePath -Raw | ConvertFrom-Json
    $projectId = $oauthConfig.installed.project_id
    if (-not $projectId) {
        $projectId = $oauthConfig.web.project_id
    }
    return $projectId
}

function Get-AbsolutePath([string]$RelativePath) {
    return [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $RelativePath))
}

function Get-SchedulerJobStatus([string]$JobName, [string]$Location, [string]$Project) {
    return (Invoke-GcloudCapture -CommandArgs @(
        "scheduler", "jobs", "describe", $JobName,
        "--location", $Location,
        "--project", $Project,
        "--format", "json(lastAttemptTime,status)"
    )) | ConvertFrom-Json
}

function Remove-FirestoreOAuthToken([string]$ProjectId, [string]$Collection, [string]$Document) {
    $accessToken = (Invoke-GcloudCapture -CommandArgs @("auth", "print-access-token", "--project", $ProjectId)).Trim()
    $uri = "https://firestore.googleapis.com/v1/projects/$ProjectId/databases/(default)/documents/$Collection/$Document"
    try {
        Invoke-RestMethod -Uri $uri -Method Delete -Headers @{ Authorization = "Bearer $accessToken" } | Out-Null
        Write-Host "Firestore token eliminado: $Collection/$Document"
    } catch {
        $status = $_.Exception.Response.StatusCode
        if ($status -eq [System.Net.HttpStatusCode]::NotFound -or [int]$status -eq 404) {
            Write-Host "Firestore token ya no existia: $Collection/$Document"
        } else {
            throw
        }
    }
}

function Wait-ForSchedulerAttempt([string]$JobName, [string]$Location, [string]$Project, [string]$PreviousAttemptTime) {
    for ($attempt = 0; $attempt -lt 15; $attempt++) {
        Start-Sleep -Seconds 2
        $jobStatus = Get-SchedulerJobStatus -JobName $JobName -Location $Location -Project $Project
        if ($jobStatus.lastAttemptTime -and ($jobStatus.lastAttemptTime -ne $PreviousAttemptTime)) {
            return $jobStatus
        }
    }

    throw "Cloud Scheduler no reporto una nueva ejecucion para $JobName."
}

function Invoke-OAuthRefresh([string]$CredentialsFilePath, [string]$TokenFilePath) {
    $pythonScript = @'
import json
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive",
]

token_path = Path(os.environ["FACTURADOR_TOKEN_PATH"])
credentials_path = Path(os.environ["FACTURADOR_CREDENTIALS_PATH"])

interactive_reauth = False
creds = None

if token_path.exists():
    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except Exception as exc:
            if "invalid_grant" not in str(exc):
                raise
            creds = None
            interactive_reauth = True
    else:
        interactive_reauth = True

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        creds = flow.run_local_server(port=0)
        interactive_reauth = True

token_path.parent.mkdir(parents=True, exist_ok=True)
token_path.write_text(creds.to_json(), encoding="utf-8")
gmail = build("gmail", "v1", credentials=creds, cache_discovery=False)
profile = gmail.users().getProfile(userId="me").execute()
print(json.dumps({
    "email": profile.get("emailAddress"),
    "interactive_reauth": interactive_reauth,
}, ensure_ascii=False))
'@

    $env:FACTURADOR_CREDENTIALS_PATH = Get-AbsolutePath $CredentialsFilePath
    $env:FACTURADOR_TOKEN_PATH = Get-AbsolutePath $TokenFilePath
    $rawOutput = $pythonScript | python -
    $jsonLine = ($rawOutput | Select-Object -Last 1)
    if (-not $jsonLine) {
        throw "No hubo salida valida del refresco OAuth."
    }
    return $jsonLine | ConvertFrom-Json
}

Assert-Tool gcloud
Assert-Tool python

if (-not (Test-Path $CredentialsPath)) {
    throw "No existe OAuth credentials: $CredentialsPath"
}

$oauthProjectId = Get-OAuthProjectId -CredentialsFilePath $CredentialsPath
if (-not $oauthProjectId) {
    throw "No se pudo obtener project_id desde $CredentialsPath"
}

Invoke-GcloudCapture -CommandArgs @("config", "set", "project", $ProjectId) | Out-Null
$activeAccount = Invoke-GcloudCapture -CommandArgs @("auth", "list", "--filter=status:ACTIVE", "--format", "value(account)")
if (-not $activeAccount) {
    throw "No hay sesion activa en gcloud. Ejecuta: gcloud auth login"
}

$authResult = Invoke-OAuthRefresh -CredentialsFilePath $CredentialsPath -TokenFilePath $TokenPath

$secretOutput = Invoke-GcloudCapture -CommandArgs @("secrets", "versions", "add", $TokenSecretName, "--data-file", $TokenPath, "--project", $ProjectId)
if ($LASTEXITCODE -ne 0) {
    throw ($secretOutput | Out-String).Trim()
}
$tokenSecretVersion = if (($secretOutput | Out-String) -match "Created version \[(\d+)\]") { $Matches[1] } else { "latest" }

# Clear the stale Firestore token so the next cold start falls back to the
# fresh Secret Manager token instead of reading the expired one from Firestore.
Remove-FirestoreOAuthToken -ProjectId $ProjectId -Collection $TokenStateCollection -Document $TokenStateDoc

$rotationTimestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
Invoke-GcloudCapture -CommandArgs @(
    "run", "services", "update", $ServiceName,
    "--region", $Region,
    "--project", $ProjectId,
    "--update-env-vars", "FACTURADOR_WATCH_TOPIC=projects/$oauthProjectId/topics/$TopicName,FACTURADOR_TOKEN_ROTATION_TS=$rotationTimestamp"
) | Out-Null

$serviceInfo = Invoke-GcloudCapture -CommandArgs @(
    "run", "services", "describe", $ServiceName,
    "--region", $Region,
    "--project", $ProjectId,
    "--format", "json(status.url,status.latestReadyRevisionName)"
)
if ($LASTEXITCODE -ne 0) {
    throw "No se pudo obtener el estado de Cloud Run."
}
$serviceStatus = $serviceInfo | ConvertFrom-Json

$watchPreviousAttemptTime = (Get-SchedulerJobStatus -JobName $WatchRenewJobName -Location $SchedulerLocation -Project $ProjectId).lastAttemptTime
Invoke-GcloudCapture -CommandArgs @("scheduler", "jobs", "run", $WatchRenewJobName, "--location", $SchedulerLocation, "--project", $ProjectId) | Out-Null
$watchJobStatus = Wait-ForSchedulerAttempt -JobName $WatchRenewJobName -Location $SchedulerLocation -Project $ProjectId -PreviousAttemptTime $watchPreviousAttemptTime
if ($watchJobStatus.status.code) {
    throw "La validacion de watch-renew fallo con codigo $($watchJobStatus.status.code)."
}

$fullSyncJobStatus = $null
if (-not $SkipFullSync) {
    $fullSyncPreviousAttemptTime = (Get-SchedulerJobStatus -JobName $FullSyncJobName -Location $SchedulerLocation -Project $ProjectId).lastAttemptTime
    Invoke-GcloudCapture -CommandArgs @("scheduler", "jobs", "run", $FullSyncJobName, "--location", $SchedulerLocation, "--project", $ProjectId) | Out-Null
    $fullSyncJobStatus = Wait-ForSchedulerAttempt -JobName $FullSyncJobName -Location $SchedulerLocation -Project $ProjectId -PreviousAttemptTime $fullSyncPreviousAttemptTime
    if ($fullSyncJobStatus.status.code) {
        throw "La validacion de full-sync fallo con codigo $($fullSyncJobStatus.status.code)."
    }
}

[pscustomobject]@{
    project_id = $ProjectId
    service = $ServiceName
    service_url = $serviceStatus.status.url
    revision = $serviceStatus.status.latestReadyRevisionName
    oauth_project_id = $oauthProjectId
    gmail_account = $authResult.email
    interactive_reauth = [bool]$authResult.interactive_reauth
    token_secret_version = $tokenSecretVersion
    watch_job = [pscustomobject]@{
        name = $WatchRenewJobName
        last_attempt_time = $watchJobStatus.lastAttemptTime
        status = "ok"
    }
    full_sync_job = if ($SkipFullSync) {
        [pscustomobject]@{
            name = $FullSyncJobName
            status = "skipped"
        }
    } else {
        [pscustomobject]@{
            name = $FullSyncJobName
            last_attempt_time = $fullSyncJobStatus.lastAttemptTime
            status = "ok"
        }
    }
} | ConvertTo-Json -Depth 6
