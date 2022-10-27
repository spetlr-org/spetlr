# This is the script that creates the entire deployment
# for readability it is split up into separate steps
# where we try to use meaningful names.
param (
  # atc-dataplatform doesn't use separate environments
  # see atc-snippets for more inspiration
  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $environmentName="",

  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $pipelineClientId,

  [Parameter(Mandatory=$false)]
  [securestring]
  $pipelineClientSecret,

  [Parameter(Mandatory=$false)]
  [string]
  $uniqueRunId
)

# import utility functions
. "$PSScriptRoot\Utilities\all.ps1"

###############################################################################################
# Execute steps in order
###############################################################################################

. "$PSScriptRoot/00-Config.ps1"
. "$PSScriptRoot/01-Verify-Arguments"
. "$PSScriptRoot/12-Provision-Databricks-Service-Principal"

Write-Host "  Deploying ressources using Bicep..." -ForegroundColor Yellow

$output = az deployment sub create `
  --location $location `
  --template-file .\.github\deploy-bicep\main.bicep `
  --parameters `
      permanentResourceGroup=$permanentResourceGroup `
      location=$location `
      keyVaultName=$keyVaultName `
      devobjectid=$devobjectid `
      spnobjectid=$spnobjectid `
      resourceTags=$resourceTags `
      cosmosName=$cosmosName `
      resourceGroupName=$resourceGroupName `
      databricksName=$databricksName `
      dataLakeName=$dataLakeName `
      datalakeContainers=$dataLakeContainersJson `
      ehNamespace=$ehNamespace `
      eventHubConfig=$eventHubConfigJson `
      databaseServerName=$databaseServerName `
      deliveryDatabase=$deliveryDatabase `
      allowUserIp=$allowUserIp `
      sqlServerAdminUser=$sqlServerAdminUser `
      sqlServerAdminPassword=$sqlServerAdminPassword `
      pipelineSpnName=$pipelineSpnName `
      pipelineObjectId=$pipelineObjectId


Throw-WhenError -output $output

Write-Host "  Ressources deployed!" -ForegroundColor Green


$eventHubConnection = az eventhubs namespace authorization-rule keys list `
  --resource-group $resourceGroupName `
  --namespace-name $ehNamespace `
  --name RootManageSharedAccessKey `
  --query primaryConnectionString `
  --output tsv

Throw-WhenError -output $eventHubConnection

Write-Host "  Saving EventHubConnection" -ForegroundColor DarkYellow
$secrets.addSecret("EventHubConnection", $eventHubConnection)

Get-ChildItem "$PSScriptRoot/steps" -Filter *.ps1 | Sort-Object name | Foreach-Object {
  . ("$_")
}
