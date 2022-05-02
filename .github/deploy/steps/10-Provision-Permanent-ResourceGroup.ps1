###############################################################################################
# Provision permanent resource group
###############################################################################################
Write-Host "Provision permanent resource group" -ForegroundColor DarkGreen

Write-Host "  Creating resource group" -ForegroundColor DarkYellow
$output = az group create `
  --name $permanentResourceGroup `
  --location $location `
  --tags $resourceTags

Throw-WhenError -output $output

Write-Host "Provision permanent resource group completed." -ForegroundColor DarkGreen
