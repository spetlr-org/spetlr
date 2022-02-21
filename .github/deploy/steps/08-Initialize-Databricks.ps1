###############################################################################################
# Initialize Databricks
###############################################################################################
Write-Host "Initialize Databricks Configuration" -ForegroundColor Green

Write-Host "  Install Databricks CLI" -ForegroundColor DarkYellow
pip install --upgrade pip --quiet
pip install --upgrade databricks-cli --quiet

Write-Host "  Add the SPN to the Databricks Workspace as an admin user" -ForegroundColor DarkYellow
$accessToken = Set-DatabricksSpnAdminUser `
  -tenantId $tenantId `
  -clientId $clientId `
  -clientSecret (ConvertFrom-SecureString $clientSecret -AsPlainText) `
  -workspaceUrl $workspaceUrl `
  -resourceId $resourceId

Write-Host "  Generate SPN personal access token" -ForegroundColor DarkYellow
$token = ConvertTo-DatabricksPersonalAccessToken `
  -workspaceUrl $workspaceUrl `
  -bearerToken $accessToken `
  -tokenComment "$tokenComment"

Write-Host "  Generate .databrickscfg" -ForegroundColor DarkYellow
Set-Content ~/.databrickscfg "[DEFAULT]"
Add-Content ~/.databrickscfg "host = https://$workspaceUrl"
Add-Content ~/.databrickscfg "token = $token"
Add-Content ~/.databrickscfg ""


