###############################################################################################
# Initialize Databricks
###############################################################################################
Write-Host "Initialize Databricks Configuration" -ForegroundColor Green

Write-Host "  Collect resourceId and workspace URL" -ForegroundColor DarkYellow
$resourceId = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query id `
  --out tsv

Throw-WhenError -output $resourceId

Write-Host "Get Databricks workspace URL" -ForegroundColor DarkYellow
$workspaceUrl = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query properties.workspaceUrl `
  --out tsv

Throw-WhenError -output $workspaceUrl
Write-Host "Workspace URL is: $workspaceUrl" -ForegroundColor DarkYellow

Write-Host "  Add the SPN to the Databricks Workspace as an admin user and get access token" -ForegroundColor DarkYellow
$accessToken = Set-DatabricksSpnAdminUser `
  -tenantId $tenantId `
  -clientId $dbSpn.clientId `
  -clientSecret $dbSpn.secretText `
  -workspaceUrl $workspaceUrl `
  -resourceId $resourceId

Write-Host " Wait 10 seconds for the Databricks admin to settle" -ForegroundColor DarkYellow
Start-Sleep -Seconds 10  

Write-Host "Convert Bearer token to Databricks personal access token" -ForegroundColor DarkYellow
$token = ConvertTo-DatabricksPersonalAccessToken `
  -workspaceUrl $workspaceUrl `
  -bearerToken $accessToken

Write-Host "Generate .databrickscfg" -ForegroundColor DarkYellow
Set-Content ~/.databrickscfg "[DEFAULT]"
Add-Content ~/.databrickscfg "host = https://$workspaceUrl"
Add-Content ~/.databrickscfg "token = $token"
Add-Content ~/.databrickscfg ""