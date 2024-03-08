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

# Check if the resource group exists
$clusterrg= "$($resourceGroupName)Cluster"
$exists = az group exists --name $clusterrg

if ($exists -eq "true") {
    Write-Host "Resource group $clusterrg exists. Deleting..."
    az group delete --name $clusterrg --yes --no-wait
    Write-Host "Deletion command sent for resource group $clusterrg."
} else {
    Write-Host "Resource group $clusterrg does not exist."
}

Write-Host "  Parent Resource Group Deleted" -ForegroundColor Green

