
$resourceGroupName           = "atcintegration"
$resourceName                = "atc"
$databricksName              = $resourceName
$dataLakeName               = $resourceName
$databaseServerName          = $resourceName + "test"
$deliveryDatabase            = "Delivery"
$ehNamespace                  = $resourceName+"namespace"


$location = "eastus" # Use eastus because of free azure subscription
$resourceTags = @(
  "Owner=Auto Deployed",
  "System=ATC-NET",
  "Service=Data Platform"
  )

$dataLakeContainers = @(
    @{name="silver"}
)

$eventHubConfig = @(
    @{
      name="atceh"
      namespace=$ehNamespace
      captureLocation = "silver"
    }
)

Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Base Configuration       *******************************************" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Resource Group                  : $resourceGroupName" -ForegroundColor White
Write-Host "* Azure Databricks Workspace      : $databricksName" -ForegroundColor White
Write-Host "* Azure Data Lake                 : $dataLakeName" -ForegroundColor White
Write-Host "* Azure SQL server                : $databaseServerName" -ForegroundColor White
Write-Host "* Azure SQL database              : $deliveryDatabase" -ForegroundColor White
Write-Host "* Azure EventHubs Namespace       : $ehNamespace" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White


$keystore = @()
$db_secrets_scope = $resourceName
