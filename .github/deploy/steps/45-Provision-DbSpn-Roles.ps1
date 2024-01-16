Write-Host "  Assigning Databricks Service Principal as Contributor for spetlr"

$output = az role assignment create `
  --role "Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $dbSpn.objectId `
  --scope "/subscriptions/$($subscriptionId)/resourceGroups/$($resourceGroupName)"

Throw-WhenError -output $output
