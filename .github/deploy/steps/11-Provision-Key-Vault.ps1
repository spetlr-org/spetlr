
#############################################################################################
# Provision key vault
#############################################################################################
Write-Host "Provision key vault" -ForegroundColor DarkGreen

Write-Host "  Query key vault" -ForegroundColor DarkYellow
$output = az keyvault show `
  --name $keyVaultName `
  --resource-group $permanentResourceGroup

if (!$?) {
  Write-Host "  Create key vault" -ForegroundColor DarkYellow
  $output = az keyvault create `
    --name $keyVaultName `
    --location $location `
    --resource-group $permanentResourceGroup `
    --sku 'standard' `
    --enabled-for-template-deployment true `
    --tags $resourceTags

  Throw-WhenError -output $output
}
else {
  Write-Host "  Key vault already exists, skipping creation" -ForegroundColor DarkYellow
}

Write-Host "  Grant access for developers" -ForegroundColor DarkYellow
$output = az keyvault set-policy `
  --name $keyVaultName `
  --resource-group $permanentResourceGroup `
  --secret-permissions list get set `
  --object-id (az account show |ConvertFrom-Json).id `
  --no-wait

Throw-WhenError -output $output

Write-Host "  Grant access for deploying spn" -ForegroundColor DarkYellow
$output = az keyvault set-policy `
  --name $keyVaultName `
  --resource-group $permanentResourceGroup `
  --secret-permissions list get set `
  --object-id (Graph-ListSpn -queryDisplayName $cicdSpnName).id

Throw-WhenError -output $output

