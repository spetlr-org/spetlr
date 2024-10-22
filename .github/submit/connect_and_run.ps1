param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]
    $uniqueRunId
)

###############################################################################################
# Get the resource group name and resource name
###############################################################################################
$resourceName = "spetlr$uniqueRunId"  # This name is also used in the Terraform section
$resourceGroupName  = $resourceName  # This name is also used in the Terraform section

Write-Host "Resource group name is $resourceGroupName"
Write-Host "Resource name is $resourceName"

###############################################################################################
# Connect to Databricks
###############################################################################################
Write-Host "Get Databricks workspace URL" -ForegroundColor Green
$workspaceUrl = az resource show `
    --resource-group $resourceGroupName `
    --name $resourceName `
    --resource-type "Microsoft.Databricks/workspaces" `
    --query properties.workspaceUrl `
    --out tsv

$workspaceUrl = "https://$workspaceUrl"
Write-Host "Workspace URL is: $workspaceUrl" -ForegroundColor DarkYellow

# Write-Host "Get Databricks captain SPN id " -ForegroundColor Green
$workspaceSpnId = az keyvault secret show `
     --vault-name $resourceName `
     --name "Captain--ClientId" `
     --query value `
     --out tsv

# Write-Host "Get Databricks captain SPN secret " -ForegroundColor Green
$workspaceSpnToken = az keyvault secret show `
    --vault-name $resourceName `
    --name "Captain--DbSecret" `
    --query value `
    --out tsv

$env:DATABRICKS_HOST = $workspaceUrl
$env:DATABRICKS_CLIENT_ID = $workspaceSpnId
$env:DATABRICKS_CLIENT_SECRET = $workspaceSpnToken

###############################################################################################
# Get the Job ID
###############################################################################################

$test_job = (databricks jobs list -o json | ConvertFrom-JSON | Where-Object {$_.settings.name -match "Test Job"}).job_id
Write-Host "Found test job with id: $test_job"

###############################################################################################
# Launch the Test Job
###############################################################################################

$token = Get-Random

# running wiht no-wait gives back the run-id
$run = databricks jobs run-now --no-wait --idempotency-token $token $test_job | ConvertFrom-JSON

$job = databricks jobs get-run $run.run_id | ConvertFrom-JSON

Write-Host $job

# re-using same idempotency-token connects to the same run
databricks jobs run-now --idempotency-token $token $test_job
$exitCode = $LASTEXITCODE

# we can now use the run_id to print the output to screen
$logs = (databricks jobs get-run-output $job.tasks.run_id |  ConvertFrom-JSON).logs
Write-Host $logs

# Restore the original exit code
exit $exitCode
