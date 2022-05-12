Write-Host "Provision Cosmos" -ForegroundColor DarkYellow

$output = az cosmosdb create `
  --name $cosmosName `
  --resource-group $permanentResourceGroup `
  --enable-free-tier true

Throw-WhenError -output $output
