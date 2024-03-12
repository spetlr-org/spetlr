# delete the entire deployment to save running costs
param (
  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $environmentName = "",

  [Parameter(Mandatory = $false)]
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

Start-Sleep -Seconds 30

# ATM, deleting the resource group above does not completely delete the managed resource group.
# Thus, we delete it here. If this is fixed by Azure, we can remove this step.
if (az group exists --name "$($resourceGroupName)Cluster" = true ) {
  Write-Host "  Managed resource group exists, Deleting it..." -ForegroundColor Green
  az group delete --name "$($resourceGroupName)Cluster" --yes --no-wait
}
else {
  Write-Host "  Managed resource group does not exist, skipping..."
}


