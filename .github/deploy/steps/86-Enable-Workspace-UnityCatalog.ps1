# THis replicates almost 1-1 the 85-create-and-connect-metastore.ps1
# $metastoreStorageAccount is already saved from previous script
# $metastoreid is already saved from precious script


Write-Host "Connecting metastore '$metastoreName' for Databricks workspace '$databricksName'" -ForegroundColor Green

$thisAccessConnector = az databricks access-connector show `
    --resource-group $resourceGroupName `
    --name $databricksAccessConnectorName `
| ConvertFrom-Json
Throw-WhenError -output $thisAccessConnector


$thisDatabricks = az databricks workspace show `
    --resource-group $resourceGroupName `
    --name $databricksName `
| ConvertFrom-Json
Throw-WhenError -output $thisDatabricks


Write-Host "Set contributer role for access connector $databricksAccessConnectorName" -ForegroundColor DarkYellow
$output = az role assignment create `
    --assignee $thisAccessConnector.identity.principalId `
    --role "Storage Blob Data Contributor" `
    --scope $metastoreStorageAccount.id
Throw-WhenError -output $output



Write-Host "Connect to Metastore Databricks Workspace. Setting SPN as admin and getting token." -ForegroundColor DarkYellow

. $PSScriptRoot/15-ensure-keyvault-access.ps1
$cicdSpn = Get-SpnWithSecret -spnName $cicdSpnName -keyVaultName $keyVaultName
Throw-WhenError -output $cicdSpn

$thisBearerToken = Set-DatabricksSpnAdminUser `
    -tenantId $tenantId `
    -clientId $cicdSpn.clientId `
    -clientSecret $cicdSpn.secretText `
    -workspaceUrl $thisDatabricks.workspaceUrl `
    -resourceId $thisDatabricks.id
Throw-WhenError -output $thisBearerToken

$thisDatabricksToken = ConvertTo-DatabricksPersonalAccessToken `
    -workspaceUrl $thisDatabricks.workspaceUrl `
    -bearerToken $thisBearerToken
Throw-WhenError -output $thisDatabricksToken

[Environment]::SetEnvironmentVariable('DATABRICKS_AAD_TOKEN', $thisDatabricksToken)

$thisDatabricksUrlHttps = "https://" + $thisDatabricks.workspaceUrl
$output = databricks configure --host $thisDatabricksUrlHttps --aad-token
Throw-WhenError -output $output


Write-Host "Assignning workspace to metastore..."
$output = databricks unity-catalog metastores assign `
    --workspace-id $thisDatabricks.workspaceId `
    --metastore-id $metastoreId
Throw-WhenError -output $output



Write-Host "Add storage credential for access connector for workspace. Check if the workspace contains the storage credential." -ForegroundColor DarkYellow
# For the metastore to have access to the access connector, a credential must be added to the workspace,
# and then the metastore can grab this credential and use it
$storageCredentials = databricks unity-catalog storage-credentials list | ConvertFrom-Json
Throw-WhenError -output $storageCredentials

if ($storageCredentials.storage_credentials.name -contains $databricksAccessConnectorName) {
    Write-Host "Does contain. Geting credential id..."
    $accessConnectorCredentialId = ($storageCredentials.storage_credentials | Where-Object { $_.name -eq $databricksAccessConnectorName }).id
}
else {
    Write-Host "Does NOT contain. Creating and get id..."
    $accessConnectorCredential = databricks unity-catalog storage-credentials create `
        --name $databricksAccessConnectorName `
        --az-mi-access-connector-id $databricksAccessConnectorName.id `
    | ConvertFrom-Json
    Throw-WhenError -output $accessConnectorCredential
    $accessConnectorCredentialId = $accessConnectorCredential.id
}


#Write-Host "Add/update storage credential to metastore" -ForegroundColor DarkYellow
#$output = databricks unity-catalog metastores update `
#  --id $metastoreId `
#  --storage-root-credential-id $accessConnectorCredentialId
#Throw-WhenError -output $output


if ($catalogs.catalogs.name -contains $databricksCatalogName) {
    Write-Host "Catalog exists"
}
else {
    Write-Host "Catalog does not exist. Creating..."
    $output = databricks unity-catalog catalogs create `
        --name $databricksCatalogName
    Throw-WhenError -output $output
}
