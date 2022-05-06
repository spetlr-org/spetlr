###############################################################################################
# Provision Databricks Workspace resources
###############################################################################################
Write-Host "Provision Databricks Workspace" -ForegroundColor DarkGreen


Write-Host "  Checking if Workspace already exists" -ForegroundColor DarkYellow
$dbWorkspace = (az databricks workspace list --query "[?name == '$($databricksName)']") | ConvertFrom-Json

If ($dbWorkspace.Count -eq 0) {
  Write-Host "  Deploying Databricks template" -ForegroundColor DarkYellow
  $output = az deployment group create `
    --resource-group $resourceGroupName `
    --template-file "$PSScriptRoot/../arm-templates/databricks-workspace.json" `
    --parameters workspaceName=$databricksName

  Throw-WhenError -output $output
}

Write-Host "  Tagging Databricks Workspace" -ForegroundColor DarkYellow
$output = az resource tag `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --tags $resourceTags

Throw-WhenError -output $output

$resourceId = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query id `
  --out tsv


Throw-WhenError -output $resourceId

$workspaceUrl = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query properties.workspaceUrl
$workspaceUrl = $workspaceUrl.Replace('"','')

Throw-WhenError -output $workspaceUrl
