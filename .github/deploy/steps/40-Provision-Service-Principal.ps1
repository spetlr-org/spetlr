Write-Host "Provision Service Principal " -ForegroundColor DarkYellow

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

$secrets.addSecret("Databricks--TenantId", (Convert-Safe-FromJson -text (az account show)).tenantId)
$secrets.addSecret("Databricks--ClientId", $mountSpn.appId)
$secrets.addSecret("Databricks--ClientSecret", $mountPassword.secretText)
