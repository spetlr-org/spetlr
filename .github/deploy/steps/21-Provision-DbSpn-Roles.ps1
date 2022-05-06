Write-Host "  Assigning Databricks Service Principal as Contributor for Atc"

$output = az role assignment create `
  --role "Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $dbDeploySpn.id `
  --resource-group $resourceGroupName

Throw-WhenError -output $output
