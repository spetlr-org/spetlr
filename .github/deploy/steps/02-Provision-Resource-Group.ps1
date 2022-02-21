###############################################################################################
# Provision resource group
###############################################################################################
Write-Host "Provision resource group" -ForegroundColor DarkGreen

Write-Host "  Creating resource group" -ForegroundColor DarkYellow
$output = az group create `
  --name $resourceGroupName `
  --location $location `
  --tags $resourceTags

Throw-WhenError -output $output

Write-Host "Provision resource group completed." -ForegroundColor DarkGreen
