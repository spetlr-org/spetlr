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

. "$PSScriptRoot\00-Config.ps1"


###############################################################################################
# Delete resource group
###############################################################################################
Write-Host "  Now Destroying Parent Resource Group!" -ForegroundColor Red

$output = az group delete --name $resourceGroupName --yes --no-wait
#Throw-WhenError -output $output

Write-Host "  Parent Resource Group Deleted" -ForegroundColor Green

###############################################################################################
# Delete Mounting App registration
###############################################################################################
Write-Host "  Now Destroying Mounting App registration!" -ForegroundColor Red

$mountApp = Graph-ListApplications -queryDisplayName $mountSpnName

if ($null -eq $mountApp)
{
  Write-Host "No application found. Already deleted?" -ForegroundColor DarkGreen
}else
{
  Write-Host "Deleting mounting app registration" -ForegroundColor DarkGreen
  Graph-DeleteApplication -appId $mountApp.id
}

