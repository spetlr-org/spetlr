Write-Host "Provision data lake" -ForegroundColor DarkGreen

Write-Host "  Creating data lake" -ForegroundColor DarkYellow
$output = az storage account create `
  --name $dataLakeName `
  --location $location `
  --resource-group $resourceGroupName `
  --encryption-service 'blob' `
  --encryption-service 'file' `
  --sku 'Standard_LRS' `
  --https-only 'true' `
  --kind 'StorageV2' `
  --hns true `
  --tags $resourceTags `
  --allow-blob-public-access false

Throw-WhenError -output $output


Write-Host "  Getting data lake storage account key" -ForegroundColor DarkYellow
$dataLakeKey = az storage account keys list `
  --account-name $dataLakeName `
  --resource-group $resourceGroupName `
  --query '[0].value' `
  --output tsv

Throw-WhenError -output $dataLakeKey

$keystore += @{
  name="Databricks--StorageAccountKey"
  key=$dataLakeKey
}

Write-Host "  Configure containers" -ForegroundColor DarkYellow
$output = az extension add `
  --name storage-preview `
  --yes

Throw-WhenError -output $output

foreach ($container in $dataLakeContainers) {
  Write-Host "  Creating $($container.name) container" -ForegroundColor DarkYellow
  $output = az storage container create `
    --name $container.name `
    --account-name $dataLakeName `
    --account-key $dataLakeKey `
    --public-access off

  Throw-WhenError -output $output

}

