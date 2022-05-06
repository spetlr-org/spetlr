Write-Host "Provision Databricks Service Principal " -ForegroundColor DarkYellow


Write-Host "Verifying Db SPN Registration"
$dbDeployApp = Graph-ListApplications -queryDisplayName $dbDeploySpnName

if ($null -eq $dbDeployApp)
{
  Write-Host "Creating Db SPN Registration" -ForegroundColor DarkGreen
  $dbDeployApp = Graph-CreateApplication -displayName $dbDeploySpnName
}else
{
  Write-Host "Databricks App registration exists" -ForegroundColor DarkGreen
}

$dbDeploySpn = Graph-ListSpn -queryDisplayName $dbDeploySpnName
if ($null -eq $dbDeploySpn)
{
  Write-Host "  Creating Db Service Principal" -ForegroundColor DarkYellow
  $dbDeploySpn = Graph-CreateSpn -appId $dbDeployApp.appId
}else
{
  Write-Host "Db Service Principal exists" -ForegroundColor DarkGreen
}

$clientSecretPlain = az keyvault secret show `
        --name="Databricks-AdminSecret" `
        --vault-name=$keyVaultName `
        --query="value" `
        --out tsv

if ($LastExitCode -eq 0) {
  Write-Host "  Got Service Principal Secret from vault" -ForegroundColor DarkYellow
}
else{
#  first check if there is an old key to delete.
  foreach($credential in $dbDeployApp.passwordCredentials){
    Graph-AppRemovePassword -keyId $credential.keyId -appId $dbDeployApp.id
  }
  Graph-ListApplications -queryDisplayName $dbDeploySpnName

  Write-Host "  Creating Service Principal Secret" -ForegroundColor DarkYellow
  $clientSecretObject = (Graph-AppAddPassword -appId $dbDeployApp.id)
  $clientSecretPlain = $clientSecretObject.secretText
  Set-KeyVaultSecret -key "Databricks-AdminSecret" `
                    -keyVaultName $keyVaultName `
                    -value $clientSecretPlain
}

$clientSecret = (ConvertTo-SecureString -AsPlainText $clientSecretPlain -Force)

$clientId = $dbDeployApp.appId
#$clientId = $dbDeploySpn.appId
$tenantId = (Convert-Safe-FromJson -text (az account show)).tenantId
