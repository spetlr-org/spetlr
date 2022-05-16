###############################################################################################
# Provision CosmosDB Secrets
###############################################################################################
Write-Host "Preparing CosmosDb secrets" -ForegroundColor Green

Write-Host "  Getting CosmosDb key" -ForegroundColor DarkYellow
$cosmosKey = az cosmosdb keys list `
  --resource-group $permanentResourceGroup `
  --name $cosmosName `
  --type keys `
  --query primaryMasterKey `
  --output tsv

Throw-WhenError -output $cosmosKey

$secrets.addSecret("Cosmos--AccountKey", $cosmosKey)

Write-Host "  Getting CosmosDb endpoint" -ForegroundColor DarkYellow
$cosmosEndpoint = az cosmosdb show `
  --resource-group $permanentResourceGroup `
  --name $cosmosName `
  --query documentEndpoint `
  --output tsv

Throw-WhenError -output $cosmosEndpoint

$values.addSecret("Cosmos--Endpoint", $cosmosEndpoint)
