
# $repoRoot = (git rev-parse --show-toplevel)

# $srcDir = "$repoRoot/tests/mount"

# Push-Location -Path $srcDir

# Write-Host "Push Setup Mounts notebook to the workspace" -ForegroundColor DarkYellow
# databricks workspace mkdirs /Workspace/Shared/Mount
# databricks workspace import-dir --overwrite .  /Workspace/Shared/Mount

# Write-Host "Create Setup Mounts job" -ForegroundColor DarkYellow
# $jobResponse = databricks jobs create --json '@job_config.json'
# $jobId = ($jobResponse | ConvertFrom-Json).job_id
# Write-Host "Job created with job_id: $jobId"

# Write-Host "Run Setup Mounts job and wait for the completion..." -ForegroundColor DarkYellow
# $runResponse = databricks jobs run-now $jobId
# $runId = ($runResponse | ConvertFrom-Json).run_id
# $runStatus = ($runResponse | ConvertFrom-Json).state.result_state
# Write-Host "Job run completed with id: $runId and status: $runStatus"

# if ($runStatus -ne "SUCCESS") {
#     Write-Host "Job run failed with id: $runId and status: $runStatus"
#     exit 1
# }