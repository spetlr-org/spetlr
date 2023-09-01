# This script sets up a number of constants.
# This step makes no call to any resource, and is therefore very fast.

if(-not $secrets){
  # allows this step to be imported multiple times.
  $secrets = [DatabricksSecretsManager]::new()
  $values = [DatabricksSecretsManager]::new()
}

# Important Paths
$repoRoot = (git rev-parse --show-toplevel)
$sqlSourceDir = Resolve-Path $PSScriptRoot/sql

$permanentResourceName       = "githubatc"
$permanentResourceGroup       = "atc-permanent"
$keyVaultName                 = "atcGithubCiCd"

# at some point, the following will be made variable between deployments
$resourceName                 = "githubspetlr$uniqueRunId"
$resourceGroupName            = $resourceName



$databricksName               = $resourceName
$dataLakeName                 = $resourceName
$databaseServerName           = $resourceName + "test"
$deliveryDatabase             = "Delivery"


$sqlServerAdminUser           = "DataPlatformAdmin"
$sqlServerAdminPassword       = Generate-Password

# Add to databrick secrets
$secrets.addSecret("SqlServer--DataPlatformAdmin", $sqlServerAdminUser)
$secrets.addSecret("SqlServer--DataPlatformAdminPassword", $sqlServerAdminPassword)


$ehNamespace                  = $resourceName

# The SPN whose role will be used to access the storage account
$mountSpnName                 = "SpetlrMountSpn"

# This SPn will be used to deploy databricks
# The reason fo using a subsidiary SPN for this is that SPN can pull a databricks
# token from an API with no human in the loop. So if the identity that runs the
# deployment is a person, using this SPN allows us to still do this.
$dbDeploySpnName              = "SpetlrDbSpn"

# The SPN that runs the github pipeline
$cicdSpnName                  = "SpetlrGithubPipe"

$cosmosName                   = $permanentResourceName



# Use eastus because of free azure subscription
# note, we no longer use a free subscription
$location                     = "westeurope"

$resourceTags = @{
  Owner='Auto Deployed'
  System='SPETLR-ORG'
  Service='Data Platform'
  deployedAt="$(Get-Date -Format "o" -AsUTC)"
}

$resourceTags = ($resourceTags| ConvertTo-Json -Depth 4 -Compress).Replace('"','\"')

$dataLakeContainers = (,@(@{"name"="silver"}))


$dataLakeContainersJson = ($dataLakeContainers | ConvertTo-Json -Depth 4 -Compress).Replace('"','\"')

$eventHubConfig = (,@(
    @{
      "name"="spetlreh"
      "namespace"=$ehNamespace
      "captureLocation" = "silver"
    }
))
$eventHubConfigJson = ($eventHubConfig | ConvertTo-Json -Depth 4 -Compress).Replace('"','\"')


$sqlAdminSpnName = $cicdSpnName

$logAnalyticsWsName = $resourceGroupName

$metastoreStorageAccountName    = "githubspetlrmetastore"
$metastoreContainerName         = "metastore"
$metastoreAccessConnectorName   = "ac-metastore"
$metastoreDatabricksName        = "dbws-metastore"
$metastoreName                  = "spetlr-metastore"
$metastoreCatalogName           = "spetlr_catalog"




Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Base Configuration       *******************************************" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Permanent Resource Group        : $permanentResourceGroup" -ForegroundColor White
Write-Host "* Permanent Resource Name         : $permanentResourceName" -ForegroundColor White
Write-Host "* Resource Group                  : $resourceGroupName" -ForegroundColor White
Write-Host "* Resource Name                   : $resourceName" -ForegroundColor White
Write-Host "* Location                        : $location" -ForegroundColor White
Write-Host "* Azure Databricks Workspace      : $databricksName" -ForegroundColor White
Write-Host "* Azure Data Lake                 : $dataLakeName" -ForegroundColor White
Write-Host "* Azure SQL Server                : $databaseServerName" -ForegroundColor White
Write-Host "* Azure SQL Database              : $deliveryDatabase" -ForegroundColor White
Write-Host "* Azure EventHubs Namespace       : $ehNamespace" -ForegroundColor White
Write-Host "* Azure CosmosDb                  : $cosmosName" -ForegroundColor White
Write-Host "* Azure Log Analytics Workspace   : $logAnalyticsWsName" -ForegroundColor White
Write-Host "* Mounting SPN                    : $mountSpnName" -ForegroundColor White
Write-Host "* Storage Account (metastore)     : $metastoreStorageAccountName" -ForegroundColor White
Write-Host "* Container (metastore)           : $metastoreContainerName" -ForegroundColor White
Write-Host "* Access Connector (metastore)    : $metastoreAccessConnectorName" -ForegroundColor White
Write-Host "* Databricks Workspace (metastore): $metastoreDatabricksName" -ForegroundColor White
Write-Host "* Databricks Metastore Name       : $metastoreName" -ForegroundColor White
Write-Host "* Metastore Catalog Name          : $metastoreCatalogName" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White



