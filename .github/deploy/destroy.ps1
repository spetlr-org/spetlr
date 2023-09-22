# delete the entire deployment to save running costs
param (
  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $environmentName="",

  [Parameter(Mandatory=$false)]
  [string]
  $uniqueRunId
)

az account show

###############################################################################################
# Configure names and options
###############################################################################################
Write-Host "Initialize deployment" -ForegroundColor Green

# import utility functions
. "$PSScriptRoot\Utilities\all.ps1"

. "$PSScriptRoot\steps\00-Config.ps1"


###############################################################################################
# Delete resource group
###############################################################################################
Write-Host "  Now Destroying Parent Resource Group!" -ForegroundColor Red

az group delete --name $resourceGroupName --yes --no-wait


Write-Host "  Parent Resource Group Deleted" -ForegroundColor Green

Write-Host "  Now Destroying Databricks Catalog: $databricksCatalogName !" -ForegroundColor Red

databricks unity-catalog catalogs delete --name $databricksCatalogName -purge

Write-Host "  Catalog deleted" -ForegroundColor Green
