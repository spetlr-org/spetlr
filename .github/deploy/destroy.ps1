# delete the entire deployment to save running costs
param (
  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $environmentName=""
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

$output = az group delete --name $resourceGroupName --yes
Throw-WhenError -output $output

Write-Host "  Parent Resource Group Deleted" -ForegroundColor Green
