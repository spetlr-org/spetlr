$secrets = [DatabricksSecretsManager]::new()
$values = [DatabricksSecretsManager]::new()


$permanentResourceName       = "githubatc"
$permanentResourceGroup       = "atc-permanent"

# at some point, the following will be made variable between deployments
$resourceName                 = "githubatc$uniqueRunId"
$resourceGroupName            = $resourceName


$databricksName               = $resourceName
$dataLakeName                 = $resourceName
$databaseServerName           = $resourceName + "test"
$deliveryDatabase             = "Delivery"


$sqlServerAdminUser           = "DataPlatformAdmin"
$sqlServerAdminPassword       = Generate-Password
$allowUserIp                  = (Invoke-WebRequest -UseBasicParsing "ifconfig.me/ip").Content.Trim()
# Add to databrick secrets
$secrets.addSecret("SqlServer--DataPlatformAdmin", $sqlServerAdminUser)
$secrets.addSecret("SqlServer--DataPlatformAdminPassword", $sqlServerAdminPassword)


$ehNamespace                  = $resourceName
$mountSpnName                 = "AtcMountSpn"
$dbDeploySpnName              = "AtcDbSpn"
$cicdSpnName                  = "AtcGithubPipe"
$cosmosName                   = $permanentResourceName
$keyVaultName                 = "atcGithubCiCd"

$location                     = "westeurope"  # Use eastus because of free azure subscription
$resourceTags = "{'Owner':'Auto Deployed', 'System':'ATC-NET','Service':'Data Platform'}"
$resourceTags = $resourceTags.Replace("'",'\"')

$dataLakeContainers = (,@(@{"name"="silver"}))


$dataLakeContainersJson = $dataLakeContainers | ConvertTo-Json -Depth 4 -Compress
$dataLakeContainersJson = $dataLakeContainersJson.Replace('"','\"')

$eventHubConfig = @(
    @{
      "name"="atceh"
      "namespace"=$ehNamespace
      "captureLocation" = "silver"
    }
)

$pipelineSpnName = $cicdSpnName
$pipelineObjectId = (Graph-ListSpn -queryDisplayName $pipelineSpnName).id

$eventHubConfigJson = "[{'name':'atceh', 'namespace':'$ehNamespace','captureLocation':'silver'}]"
$eventHubConfigJson = $eventHubConfigJson.Replace("'",'\"')


$devobjectid = az account show --query id

$spnobjectid = (Graph-ListSpn -queryDisplayName $cicdSpnName).id




Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Base Configuration       *******************************************" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Permanent Resource Group        : $permanentResourceGroup" -ForegroundColor White
Write-Host "* Permanent Resource Name         : $permanentResourceName" -ForegroundColor White
Write-Host "* Resource Group                  : $resourceGroupName" -ForegroundColor White
Write-Host "* Resource Name                   : $resourceName" -ForegroundColor White
Write-Host "* location                        : $location" -ForegroundColor White
Write-Host "* Azure Databricks Workspace      : $databricksName" -ForegroundColor White
Write-Host "* Azure Data Lake                 : $dataLakeName" -ForegroundColor White
Write-Host "* Azure SQL server                : $databaseServerName" -ForegroundColor White
Write-Host "* Azure SQL database              : $deliveryDatabase" -ForegroundColor White
Write-Host "* Azure EventHubs Namespace       : $ehNamespace" -ForegroundColor White
Write-Host "* Azure CosmosDb name             : $cosmosName" -ForegroundColor White
Write-Host "* Mounting SPN Name               : $mountSpnName" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White



