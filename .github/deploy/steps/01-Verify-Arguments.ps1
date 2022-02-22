
Write-Host "Verifying powershell arguments" -ForegroundColor DarkGreen

Write-Host "  Use Azure AD OAuth tokens" -ForegroundColor DarkYellow
if (!$clientSecret) {
  Write-Error "  Missing Client Secret"
  throw
}
if ((ConvertFrom-SecureString $clientSecret -AsPlainText) -eq "") {
  Write-Error "  Missing Client Secret"
  throw
}

if (!$tenantId) {
  $tenantId = (az account show --query tenantId).Replace('"','')
}

# Allow dynamic install of extensions to be able to use az databricks commands
az config set extension.use_dynamic_install=yes_without_prompt

