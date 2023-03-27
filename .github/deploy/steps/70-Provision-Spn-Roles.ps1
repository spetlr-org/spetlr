Write-Host "  Assigning Service Principal as Contributor for Spetlr"

$output = az role assignment create `
  --role "Storage Blob Data Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $mountSpn.objectId `
  --resource-group $resourceGroupName

Throw-WhenError -output $output
