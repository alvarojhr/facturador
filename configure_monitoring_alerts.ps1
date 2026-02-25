param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectId,
    [string]$ServiceName = "facturador-gmail-trigger",
    [string]$WatchRenewJobName = "facturador-watch-renew",
    [string]$FullSyncJobName = "facturador-full-sync"
)

$ErrorActionPreference = "Continue"

function Get-GcloudExecutable() {
    $defaultGcloudBin = Join-Path $env:LOCALAPPDATA "Google\Cloud SDK\google-cloud-sdk\bin"
    $defaultGcloudCmd = Join-Path $defaultGcloudBin "gcloud.cmd"
    if (Test-Path $defaultGcloudCmd) {
        return $defaultGcloudCmd
    }
    return "gcloud"
}

$script:GcloudExe = Get-GcloudExecutable

function Run-Gcloud(
    [string[]]$CommandArgs,
    [switch]$IgnoreExitCode
) {
    $output = & $script:GcloudExe @CommandArgs 2>&1
    if (-not $IgnoreExitCode -and $LASTEXITCODE -ne 0) {
        throw ($output -join "`n")
    }
    return $output
}

function Ensure-LogMetric(
    [string]$Project,
    [string]$MetricName,
    [string]$Description,
    [string]$Filter
) {
    Run-Gcloud -CommandArgs @("logging", "metrics", "describe", $MetricName, "--project", $Project) -IgnoreExitCode | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Run-Gcloud -CommandArgs @(
            "logging", "metrics", "create", $MetricName,
            "--project", $Project,
            "--description", $Description,
            "--log-filter", $Filter
        ) | Out-Null
        Write-Output "Metrica creada: $MetricName"
        return
    }

    Run-Gcloud -CommandArgs @(
        "logging", "metrics", "update", $MetricName,
        "--project", $Project,
        "--description", $Description,
        "--log-filter", $Filter
    ) | Out-Null
    Write-Output "Metrica actualizada: $MetricName"
}

function Ensure-AlertPolicy(
    [string]$Project,
    [string]$DisplayName,
    [hashtable]$PolicyObject
) {
    $tmp = Join-Path $env:TEMP ("facturador_alert_" + [Guid]::NewGuid().ToString("N") + ".json")
    [System.IO.File]::WriteAllText($tmp, ($PolicyObject | ConvertTo-Json -Depth 30), (New-Object System.Text.UTF8Encoding($false)))

    $existingRaw = Run-Gcloud -CommandArgs @("monitoring", "policies", "list", "--project", $Project, "--format", "json")
    $existing = $existingRaw | ConvertFrom-Json
    $match = $existing | Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
    if ($match) {
        Run-Gcloud -CommandArgs @("monitoring", "policies", "update", $match.name, "--project", $Project, "--policy-from-file", $tmp) | Out-Null
        Write-Output "Policy actualizada: $DisplayName"
    } else {
        Run-Gcloud -CommandArgs @("monitoring", "policies", "create", "--project", $Project, "--policy-from-file", $tmp) | Out-Null
        Write-Output "Policy creada: $DisplayName"
    }

    Remove-Item $tmp -ErrorAction SilentlyContinue
}

Run-Gcloud -CommandArgs @("config", "set", "project", $ProjectId) | Out-Null
Run-Gcloud -CommandArgs @("services", "enable", "logging.googleapis.com", "monitoring.googleapis.com", "--project", $ProjectId) | Out-Null

$invalidGrantMetric = "facturador_invalid_grant_count"
$schedulerFailuresMetric = "facturador_scheduler_failures_count"
$healthDegradedMetric = "facturador_health_degraded_count"

Ensure-LogMetric `
    -Project $ProjectId `
    -MetricName $invalidGrantMetric `
    -Description "Conteo de invalid_grant en Cloud Run Facturador" `
    -Filter "resource.type=`"cloud_run_revision`" AND resource.labels.service_name=`"$ServiceName`" AND textPayload:`"invalid_grant`""

Ensure-LogMetric `
    -Project $ProjectId `
    -MetricName $schedulerFailuresMetric `
    -Description "Conteo de ejecuciones fallidas de Scheduler para Facturador" `
    -Filter "resource.type=`"cloud_scheduler_job`" AND (resource.labels.job_id=`"$WatchRenewJobName`" OR resource.labels.job_id=`"$FullSyncJobName`") AND jsonPayload.status:*"

Ensure-LogMetric `
    -Project $ProjectId `
    -MetricName $healthDegradedMetric `
    -Description "Conteo de healthz degradado en Facturador" `
    -Filter "resource.type=`"cloud_run_revision`" AND resource.labels.service_name=`"$ServiceName`" AND textPayload:`"facturador_health_degraded`""

$commonAggregation = @{
    alignmentPeriod = "300s"
    perSeriesAligner = "ALIGN_DELTA"
    crossSeriesReducer = "REDUCE_SUM"
}

$policyInvalidGrant = @{
    displayName = "Facturador - OAuth invalid_grant"
    combiner = "OR"
    enabled = $true
    documentation = @{
        content = "Facturador detecto invalid_grant en OAuth. Requiere rotar token en Secret Manager."
        mimeType = "text/markdown"
    }
    conditions = @(
        @{
            displayName = "invalid_grant en logs"
            conditionThreshold = @{
                filter = "metric.type=`"logging.googleapis.com/user/$invalidGrantMetric`" AND resource.type=`"cloud_run_revision`""
                comparison = "COMPARISON_GT"
                thresholdValue = 0
                duration = "0s"
                trigger = @{ count = 1 }
                aggregations = @($commonAggregation)
            }
        }
    )
}

$policySchedulerFailures = @{
    displayName = "Facturador - Scheduler failures"
    combiner = "OR"
    enabled = $true
    documentation = @{
        content = "Facturador detecto fallos de Scheduler (watch-renew/full-sync)."
        mimeType = "text/markdown"
    }
    conditions = @(
        @{
            displayName = "fallos Scheduler detectados"
            conditionThreshold = @{
                filter = "metric.type=`"logging.googleapis.com/user/$schedulerFailuresMetric`" AND resource.type=`"cloud_scheduler_job`""
                comparison = "COMPARISON_GT"
                thresholdValue = 0
                duration = "0s"
                trigger = @{ count = 1 }
                aggregations = @($commonAggregation)
            }
        }
    )
}

$policyHealthDegraded = @{
    displayName = "Facturador - Health degraded"
    combiner = "OR"
    enabled = $true
    documentation = @{
        content = "Facturador reporto automation_ready=false en /healthz."
        mimeType = "text/markdown"
    }
    conditions = @(
        @{
            displayName = "health degradado detectado"
            conditionThreshold = @{
                filter = "metric.type=`"logging.googleapis.com/user/$healthDegradedMetric`" AND resource.type=`"cloud_run_revision`""
                comparison = "COMPARISON_GT"
                thresholdValue = 0
                duration = "0s"
                trigger = @{ count = 1 }
                aggregations = @($commonAggregation)
            }
        }
    )
}

Ensure-AlertPolicy -Project $ProjectId -DisplayName $policyInvalidGrant.displayName -PolicyObject $policyInvalidGrant
Ensure-AlertPolicy -Project $ProjectId -DisplayName $policySchedulerFailures.displayName -PolicyObject $policySchedulerFailures
Ensure-AlertPolicy -Project $ProjectId -DisplayName $policyHealthDegraded.displayName -PolicyObject $policyHealthDegraded

Write-Output "Monitoreo de Facturador configurado."
