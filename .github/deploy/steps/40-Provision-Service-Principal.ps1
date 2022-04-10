Write-Host "Provision Service Principal " -ForegroundColor DarkYellow
# See naming standard: https://dev.azure.com/cleveras/Clever/_wiki/wikis/Clever.Wiki/892/Naming-Standard




Write-Host "Verifying SPN Registration"
$mountApp = Graph-ListApplications -queryDisplayName $mountSpnName

if ($null -eq $mountApp)
{
  Write-Host "Creating SPN Registration" -ForegroundColor DarkGreen
  $mountApp = Graph-CreateApplication -displayName $mountSpnName
}else
{
  Write-Host "Mounting App registration exists" -ForegroundColor DarkGreen
}

$mountSpn = Graph-ListSpn -queryDisplayName $mountSpnName
if ($null -eq $mountSpn)
{
  Write-Host "  Creating Service Principal" -ForegroundColor DarkYellow
  $mountSpn = Graph-CreateSpn -appId $mountApp.appId
}else
{
  Write-Host "Mounting Service Principal exists" -ForegroundColor DarkGreen
}


Write-Host "  Creating Service Principal Secret" -ForegroundColor DarkYellow
$mountPassword = Graph-AppAddPassword -appId $mountApp.id


$keystore += @{
  name="Databricks--StorageAccountKey"
  key=$mountPassword.secretText
}

$keystore += @{
  name="Databricks--TenantId"
  key=(Convert-Safe-FromJson -text (az account show)).tenantId
}

$keystore += @{
  name="Databricks--ClientId"
  key=$mountSpn.appId
}
