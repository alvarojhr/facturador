param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,
    [string]$Region = "us-central1",
    [string]$ServiceName = "facturador-gmail-trigger",
    [string]$FirestoreLocation = "us-central1",
    [string]$TopicName = "facturador-gmail-updates",
    [string]$SubscriptionName = "facturador-gmail-push",
    [string]$SchedulerLocation = "us-central1",
    [string]$WatchRenewJobName = "facturador-watch-renew",
    [string]$FullSyncJobName = "facturador-full-sync",
    [string]$WatchSchedule = "0 */6 * * *",
    [string]$WatchScheduleTimeZone = "Etc/UTC",
    [string]$FullSyncSchedule = "0 2 * * *",
    [string]$FullSyncScheduleTimeZone = "America/Bogota",
    [int]$FullSyncMaxCycles = 2,
    [string]$TriggerServiceAccountName = "facturador-trigger-sa",
    [string]$SchedulerServiceAccountName = "facturador-scheduler-sa",
    [string]$PubSubPushServiceAccountName = "facturador-pubsub-push-sa",
    [string]$StateCollection = "facturador_state",
    [string]$StateDoc = "gmail_watch",
    [string]$WatchLabelIds = "INBOX",
    [string]$WatchSyncAfterStart = "false",
    [int]$SyncMaxCycles = 5,
    [string]$ConfigPath = "config/mail_automation.json",
    [string]$CredentialsPath = "config/google_credentials.json",
    [string]$TokenPath = "config/google_token.json",
    [string]$ConfigSecretName = "facturador-mail-automation-config",
    [string]$CredentialsSecretName = "facturador-google-credentials",
    [string]$TokenSecretName = "facturador-google-token",
    [string]$AdminTokenSecretName = "facturador-admin-token",
    [string]$AdminToken = ""
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

function Test-GcloudResource {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    try {
        & $gcloudExe @Args *> $null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

function Ensure-ServiceAccount([string]$Project, [string]$Name) {
    $email = "$Name@$Project.iam.gserviceaccount.com"
    if (-not (Test-GcloudResource -Args @("iam", "service-accounts", "describe", $email, "--project", $Project))) {
        gcloud iam service-accounts create $Name --project $Project | Out-Null
    }
    return $email
}

function Ensure-SecretWithFile([string]$Project, [string]$SecretName, [string]$FilePath) {
    if (-not (Test-GcloudResource -Args @("secrets", "describe", $SecretName, "--project", $Project))) {
        gcloud secrets create $SecretName --replication-policy automatic --project $Project | Out-Null
    }
    gcloud secrets versions add $SecretName --data-file $FilePath --project $Project | Out-Null
}

function Ensure-SecretWithText([string]$Project, [string]$SecretName, [string]$Text) {
    $tmp = Join-Path $env:TEMP "$SecretName.txt"
    [System.IO.File]::WriteAllText($tmp, $Text, (New-Object System.Text.UTF8Encoding($false)))
    Ensure-SecretWithFile -Project $Project -SecretName $SecretName -FilePath $tmp
    Remove-Item $tmp -ErrorAction SilentlyContinue
}

function Ensure-Topic([string]$Project, [string]$TopicName) {
    if (-not (Test-GcloudResource -Args @("pubsub", "topics", "describe", $TopicName, "--project", $Project))) {
        gcloud pubsub topics create $TopicName --project $Project | Out-Null
    }
}

function Ensure-Subscription(
    [string]$Project,
    [string]$SubscriptionName,
    [string]$TopicName,
    [string]$PushEndpoint,
    [string]$PushServiceAccount
) {
    if (-not (Test-GcloudResource -Args @("pubsub", "subscriptions", "describe", $SubscriptionName, "--project", $Project))) {
        gcloud pubsub subscriptions create $SubscriptionName `
            --project $Project `
            --topic $TopicName `
            --push-endpoint "$PushEndpoint" `
            --push-auth-service-account "$PushServiceAccount" `
            --ack-deadline 30 | Out-Null
    } else {
        gcloud pubsub subscriptions update $SubscriptionName `
            --project $Project `
            --push-endpoint "$PushEndpoint" `
            --push-auth-service-account "$PushServiceAccount" | Out-Null
    }
}

function Ensure-SchedulerJob(
    [string]$Project,
    [string]$Location,
    [string]$JobName,
    [string]$Schedule,
    [string]$TimeZone,
    [string]$Uri,
    [string]$ServiceAccount,
    [string]$Audience,
    [string]$HeaderValue
) {
    if (-not (Test-GcloudResource -Args @("scheduler", "jobs", "describe", $JobName, "--location", $Location, "--project", $Project))) {
        gcloud scheduler jobs create http $JobName `
            --project $Project `
            --location $Location `
            --schedule "$Schedule" `
            --time-zone "$TimeZone" `
            --uri "$Uri" `
            --http-method POST `
            --oidc-service-account-email "$ServiceAccount" `
            --oidc-token-audience "$Audience" `
            --headers "X-Facturador-Admin-Token=$HeaderValue" | Out-Null
    } else {
        gcloud scheduler jobs update http $JobName `
            --project $Project `
            --location $Location `
            --schedule "$Schedule" `
            --time-zone "$TimeZone" `
            --uri "$Uri" `
            --http-method POST `
            --oidc-service-account-email "$ServiceAccount" `
            --oidc-token-audience "$Audience" `
            --headers "X-Facturador-Admin-Token=$HeaderValue" | Out-Null
    }
}

Assert-Tool gcloud

if (-not (Test-Path $ConfigPath)) {
    throw "No existe config de automatizacion: $ConfigPath"
}
if (-not (Test-Path $CredentialsPath)) {
    throw "No existe OAuth credentials: $CredentialsPath"
}
if (-not (Test-Path $TokenPath)) {
    throw "No existe OAuth token: $TokenPath"
}

& $gcloudExe config set project $ProjectId | Out-Null

$activeAccount = & $gcloudExe auth list --filter=status:ACTIVE --format "value(account)"
if (-not $activeAccount) {
    throw "No hay sesion activa en gcloud. Ejecuta: gcloud auth login"
}

$projectNumber = & $gcloudExe projects describe $ProjectId --format "value(projectNumber)"
if (-not $projectNumber) {
    throw "No se pudo obtener project number de $ProjectId"
}

gcloud services enable `
    run.googleapis.com `
    cloudbuild.googleapis.com `
    artifactregistry.googleapis.com `
    pubsub.googleapis.com `
    secretmanager.googleapis.com `
    firestore.googleapis.com `
    cloudscheduler.googleapis.com `
    iam.googleapis.com `
    gmail.googleapis.com `
    --project $ProjectId | Out-Null

if (-not (Test-GcloudResource -Args @("firestore", "databases", "describe", "--database=(default)", "--project", $ProjectId))) {
    gcloud firestore databases create `
        --database="(default)" `
        --location="$FirestoreLocation" `
        --type=firestore-native `
        --project $ProjectId | Out-Null
}

$triggerServiceAccount = Ensure-ServiceAccount -Project $ProjectId -Name $TriggerServiceAccountName
$schedulerServiceAccount = Ensure-ServiceAccount -Project $ProjectId -Name $SchedulerServiceAccountName
$pubsubPushServiceAccount = Ensure-ServiceAccount -Project $ProjectId -Name $PubSubPushServiceAccountName

gcloud projects add-iam-policy-binding $ProjectId `
    --member "serviceAccount:$triggerServiceAccount" `
    --role "roles/secretmanager.secretAccessor" | Out-Null
gcloud projects add-iam-policy-binding $ProjectId `
    --member "serviceAccount:$triggerServiceAccount" `
    --role "roles/datastore.user" | Out-Null

Ensure-Topic -Project $ProjectId -TopicName $TopicName

if (-not $AdminToken) {
    $AdminToken = [Guid]::NewGuid().ToString("N")
}

$cloudConfigObj = Get-Content $ConfigPath -Raw | ConvertFrom-Json
$cloudConfigObj.credentials_path = "/secrets/google_credentials.json"
$cloudConfigObj.token_path = "/secrets/google_token.json"
$cloudConfigObj.local_work_dir = "/tmp/facturador"
$cloudConfigObj.max_messages_per_poll = 100
$cloudConfigText = $cloudConfigObj | ConvertTo-Json -Depth 12
$tmpCloudConfig = Join-Path $env:TEMP "facturador_mail_automation_cloud.json"
[System.IO.File]::WriteAllText($tmpCloudConfig, "$cloudConfigText`n", (New-Object System.Text.UTF8Encoding($false)))

Ensure-SecretWithFile -Project $ProjectId -SecretName $ConfigSecretName -FilePath $tmpCloudConfig
Ensure-SecretWithFile -Project $ProjectId -SecretName $CredentialsSecretName -FilePath $CredentialsPath
Ensure-SecretWithFile -Project $ProjectId -SecretName $TokenSecretName -FilePath $TokenPath
Ensure-SecretWithText -Project $ProjectId -SecretName $AdminTokenSecretName -Text $AdminToken
Remove-Item $tmpCloudConfig -ErrorAction SilentlyContinue

gcloud run deploy $ServiceName `
    --project $ProjectId `
    --region $Region `
    --source . `
    --platform managed `
    --service-account "$triggerServiceAccount" `
    --no-allow-unauthenticated `
    --concurrency 1 `
    --min-instances 0 `
    --max-instances 1 `
    --port 8080 `
    --labels "app=facturador,component=mail-trigger,environment=preprod" `
    --set-env-vars "FACTURADOR_AUTOMATION_CONFIG_PATH=/secrets/config/mail_automation.json,FACTURADOR_CREDENTIALS_PATH=/secrets/oauth/google_credentials.json,FACTURADOR_TOKEN_PATH=/secrets/token/google_token.json,FACTURADOR_STATE_COLLECTION=$StateCollection,FACTURADOR_STATE_DOC=$StateDoc,FACTURADOR_WATCH_TOPIC=projects/$ProjectId/topics/$TopicName,FACTURADOR_WATCH_LABEL_IDS=$WatchLabelIds,FACTURADOR_WATCH_SYNC_AFTER_START=$WatchSyncAfterStart,FACTURADOR_SYNC_MAX_CYCLES=$SyncMaxCycles" `
    --set-secrets "FACTURADOR_ADMIN_TOKEN=$AdminTokenSecretName:latest" `
    --set-secrets "/secrets/config/mail_automation.json=$ConfigSecretName:latest" `
    --set-secrets "/secrets/oauth/google_credentials.json=$CredentialsSecretName:latest" `
    --set-secrets "/secrets/token/google_token.json=$TokenSecretName:latest" `
    --quiet | Out-Null

$serviceUrl = gcloud run services describe $ServiceName --project $ProjectId --region $Region --format "value(status.url)"
if (-not $serviceUrl) {
    throw "No se pudo obtener URL de Cloud Run."
}

gcloud run services add-iam-policy-binding $ServiceName `
    --project $ProjectId `
    --region $Region `
    --member "serviceAccount:$schedulerServiceAccount" `
    --role "roles/run.invoker" | Out-Null

gcloud run services add-iam-policy-binding $ServiceName `
    --project $ProjectId `
    --region $Region `
    --member "serviceAccount:$pubsubPushServiceAccount" `
    --role "roles/run.invoker" | Out-Null

$pubsubServiceAgent = "service-$projectNumber@gcp-sa-pubsub.iam.gserviceaccount.com"
gcloud iam service-accounts add-iam-policy-binding $pubsubPushServiceAccount `
    --project $ProjectId `
    --member "serviceAccount:$pubsubServiceAgent" `
    --role "roles/iam.serviceAccountTokenCreator" | Out-Null

Ensure-Subscription `
    -Project $ProjectId `
    -SubscriptionName $SubscriptionName `
    -TopicName $TopicName `
    -PushEndpoint "$serviceUrl/pubsub/push" `
    -PushServiceAccount $pubsubPushServiceAccount

Ensure-SchedulerJob `
    -Project $ProjectId `
    -Location $SchedulerLocation `
    -JobName $WatchRenewJobName `
    -Schedule $WatchSchedule `
    -TimeZone $WatchScheduleTimeZone `
    -Uri "$serviceUrl/admin/start-watch" `
    -ServiceAccount $schedulerServiceAccount `
    -Audience $serviceUrl `
    -HeaderValue $AdminToken

Ensure-SchedulerJob `
    -Project $ProjectId `
    -Location $SchedulerLocation `
    -JobName $FullSyncJobName `
    -Schedule $FullSyncSchedule `
    -TimeZone $FullSyncScheduleTimeZone `
    -Uri "$serviceUrl/admin/full-sync?max_cycles=$FullSyncMaxCycles" `
    -ServiceAccount $schedulerServiceAccount `
    -Audience $serviceUrl `
    -HeaderValue $AdminToken

$identityToken = gcloud auth print-identity-token --audiences "$serviceUrl"
$headers = @{
    "Authorization" = "Bearer $identityToken"
    "X-Facturador-Admin-Token" = $AdminToken
}
$initialWatch = Invoke-RestMethod -Method Post -Uri "$serviceUrl/admin/start-watch" -Headers $headers

Write-Output "Deployment completado."
Write-Output "Cloud Run URL: $serviceUrl"
Write-Output "Pub/Sub topic: projects/$ProjectId/topics/$TopicName"
Write-Output "Pub/Sub subscription: $SubscriptionName"
Write-Output "Watch renew job: $WatchRenewJobName"
Write-Output "Full sync job: $FullSyncJobName"
Write-Output "Admin token usado: $AdminToken"
Write-Output "Respuesta start-watch inicial:"
$initialWatch | ConvertTo-Json -Depth 8
