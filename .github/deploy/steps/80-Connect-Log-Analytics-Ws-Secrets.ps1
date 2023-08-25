###############################################################################################
# Provision Log Analytics Ws Secrets
###############################################################################################
Write-Host "Preparing Log Analytics Ws secrets" -ForegroundColor Green

Write-Host "  Getting Workspace ID" -ForegroundColor DarkYellow
$logAnalyticsWs = az monitor log-analytics workspace show `
  --resource-group $resourceGroupName `
  --name $logAnalyticsWsName

Throw-WhenError -output $logAnalyticsWs

$logAnalyticsWs = $logAnalyticsWs | ConvertFrom-Json

$logAnalyticsWsId = $logAnalyticsWs.customerId

$secrets.addSecret("LogAnalyticsWs--WorkspaceID", $logAnalyticsWsId)

Write-Host "  Getting Shared keys" -ForegroundColor DarkYellow

$logAnalyticsWsSharedKeys = az monitor log-analytics workspace get-shared-keys `
  --resource-group $resourceGroupName `
  --name $logAnalyticsWsName

Throw-WhenError -output $logAnalyticsWs

$logAnalyticsWsSharedKeys = $logAnalyticsWsSharedKeys | ConvertFrom-Json

$logAnalyticsWsPrimaryKey = $logAnalyticsWsSharedKeys.primarySharedKey
$logAnalyticsWsSecondaryKey = $logAnalyticsWsSharedKeys.secondarySharedKey

$secrets.addSecret("LogAnalyticsWs--PrimaryKey", $logAnalyticsWsPrimaryKey)
$secrets.addSecret("LogAnalyticsWs--SecondaryKey", $logAnalyticsWsSecondaryKey)

