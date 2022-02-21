
$resourceGroupName           = "atcintegration"
$resourceName                = "atc"
$databricksName              = $resourceName

$location = "westeurope"
$resourceTags = @(
  "Owner=Auto Deployed",
  "System=ATC-NET",
  "Service=Data Platform"
  )

Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Base Configuration       *******************************************" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Resource Group                  : $resourceGroupName" -ForegroundColor White
Write-Host "* Azure Databricks Workspace      : $databricksName" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White

