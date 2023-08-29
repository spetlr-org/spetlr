Write-Host "  Assigning Databricks Service Principal as Contributor for spetlr"

$output = az role assignment create `
  --role "Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $dbSpn.objectId `
  --resource-group $resourceGroupName `
  --scope "/subscriptions/$($account.id)/resourceGroups/$resourceGroupName"

Throw-WhenError -output $output
