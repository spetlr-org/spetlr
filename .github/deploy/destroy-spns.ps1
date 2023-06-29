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
# Delete spns
###############################################################################################
Write-Host "  Now destroying SPN $dbDeploySpnName !" -ForegroundColor Red
$appId = az ad app list `
  --display-name $dbDeploySpnName `
  --query [-1].appId `
  --out tsv

Graph-DeleteApplication --appId $appId

Write-Host "  Now destroying SPN $mountSpnName !" -ForegroundColor Red
$appId = az ad app list `
  --display-name $mountSpnName `
  --query [-1].appId `
  --out tsv

Graph-DeleteApplication --appId $appId

###############################################################################################
# Purge secrets
###############################################################################################

Write-Host "  Now purging secret for $dbDeploySpnName !" -ForegroundColor Red
az keyvault secret purge --name $dbDeploySpnName --vault-name $keyVaultName

Write-Host "  Now purging secret for $mountSpnName !" -ForegroundColor Red
az keyvault secret purge --name $mountSpnName --vault-name $keyVaultName

Write-Host "  SPNs Deleted" -ForegroundColor Green