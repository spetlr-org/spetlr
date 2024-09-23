# This script sets up a number of constants.
# This step makes no call to any resource, and is therefore very fast.

if (-not $secrets) {
  # allows this step to be imported multiple times.
  $secrets = [DatabricksSecretsManager]::new()
  $values = [DatabricksSecretsManager]::new()
}

# Important Paths
$sqlSourceDir = Resolve-Path $PSScriptRoot/sql


# at some point, the following will be made variable between deployments
$resourceName = "githubspetlr$uniqueRunId"
$resourceGroupName = $resourceName

$databaseServerName = $resourceName
$deliveryDatabase = "Delivery"

$sqlServerAdminUser = "DataPlatformAdmin"
$sqlServerAdminPassword = Generate-Password

# Add to databrick secrets
$secrets.addSecret("SqlServer--DataPlatformAdmin", $sqlServerAdminUser)
$secrets.addSecret("SqlServer--DataPlatformAdminPassword", $sqlServerAdminPassword)


# The SPN that runs the github pipeline
$cicdSpnName = "SpetlrGithubPipe"


Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Base Configuration       *******************************************" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Permanent Resource Name         : $permanentResourceName" -ForegroundColor White
Write-Host "* Resource Group                  : $resourceGroupName" -ForegroundColor White
Write-Host "* Resource Name                   : $resourceName" -ForegroundColor White
Write-Host "* Azure SQL Server                : $databaseServerName" -ForegroundColor White
Write-Host "* Azure SQL Database              : $deliveryDatabase" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
