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
