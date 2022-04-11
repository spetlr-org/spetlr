Write-Host "  Assigning Service Principal as Contributor for Atc"

$output = az role assignment create `
  --role "Storage Blob Data Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $mountSpn.id `
  --resource-group $resourceGroupName

Throw-WhenError -output $output
