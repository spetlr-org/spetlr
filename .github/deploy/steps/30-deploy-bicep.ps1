
Write-Host "  Deploying ressources using Bicep..." -ForegroundColor Yellow

# the following settings were moved here from 00-Config to keep the config file free of any
# command that is slow or that can fail. This is to keep the destroy script, which also
# depends on 00-Config as fast and as reliable as possible.
$allowUserIp                  = (Invoke-WebRequest -UseBasicParsing "ifconfig.me/ip").Content.Trim()
$spnobjectid = (Graph-GetSpn -queryDisplayName $cicdSpnName).id

$sqlAdminSpnName=$dbSpn.name
$sqlAdminObjectId=$dbSpn.objectId

$output = az deployment sub create `
  --location $location `
  --template-file $repoRoot\.github\deploy-bicep\main.bicep `
  --parameters `
      permanentResourceGroup=$permanentResourceGroup `
      location=$location `
      keyVaultName=$keyVaultName `
      spnobjectid=$spnobjectid `
      resourceTags="$($resourceTagsJson)" `
      cosmosName=$cosmosName `
      resourceGroupName=$resourceGroupName `
      databricksName=$databricksName `
      dataLakeName=$dataLakeName `
      datalakeContainers="$($dataLakeContainersJson)" `
      ehNamespace=$ehNamespace `
      eventHubConfig="$($eventHubConfigJson)" `
      databaseServerName=$databaseServerName `
      deliveryDatabase=$deliveryDatabase `
      allowUserIp=$allowUserIp `
      sqlServerAdminUser=$sqlServerAdminUser `
      sqlServerAdminPassword=$sqlServerAdminPassword `
      sqlAdminSpnName=$sqlAdminSpnName `
      sqlAdminObjectId=$sqlAdminObjectId `
      logAnalyticsWsName=$logAnalyticsWsName `
      metastoreStorageAccountName=$metastoreStorageAccountName `
      metastoreContainerName=$metastoreContainerName `
      metastoreAccessConnectorName=$metastoreAccessConnectorName `
      metastoreDatabricksName=$metastoreDatabricksName


Throw-WhenError -output $output

Write-Host "  Ressources deployed!" -ForegroundColor Green
