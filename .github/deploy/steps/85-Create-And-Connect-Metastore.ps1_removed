###############################################################################################
# Metastore: 
# 1. Get resource information from azure on: storage account, access connector, and databricks workspace
# 2. Set contributer role for access connector
# 3. Connect to workspace 
# 4. Create metastore 
# 5. Assign databricks workspace to metastore
# 6. Add storage credential for access connector for workspace
# 7. Add storage credential to metastore
# 8. Create catalog
###############################################################################################
Write-Host "Creating and connecting metastore '$metastoreName' for Databricks workspace '$databricksName'" -ForegroundColor Green

Write-Host "Get resource information: Metastore Storage Account, Access Connector, Metastore Databricks Workspace" -ForegroundColor DarkYellow
$metastoreStorageAccount = az storage account show `
  --resource-group $permanentResourceGroup `
  --name $metastoreStorageAccountName `
  | ConvertFrom-Json
Throw-WhenError -output $metastoreStorageAccount

$metastoreAccessConnector = az databricks access-connector show `
  --resource-group $permanentResourceGroup `
  --name $metastoreAccessConnectorName `
  | ConvertFrom-Json
Throw-WhenError -output $metastoreAccessConnector

$metastoreDatabricks = az databricks workspace show `
  --resource-group $permanentResourceGroup `
  --name $metastoreDatabricksName `
  | ConvertFrom-Json
Throw-WhenError -output $metastoreDatabricks

Write-Host "Set contributer role for access connector" -ForegroundColor DarkYellow
$output = az role assignment create `
  --assignee $metastoreAccessConnector.identity.principalId `
  --role "Storage Blob Data Contributor" `
  --scope $metastoreStorageAccount.id
Throw-WhenError -output $output

Write-Host "Connect to Metastore Databricks Workspace. Setting SPN as admin and getting token." -ForegroundColor DarkYellow

. $PSScriptRoot/15-ensure-keyvault-access.ps1
$cicdSpn = Get-SpnWithSecret -spnName $cicdSpnName -keyVaultName $keyVaultName
Throw-WhenError -output $cicdSpn

$metastoreBearerToken = Set-DatabricksSpnAdminUser `
  -tenantId $tenantId `
  -clientId $cicdSpn.clientId `
  -clientSecret $cicdSpn.secretText `
  -workspaceUrl $metastoreDatabricks.workspaceUrl `
  -resourceId $metastoreDatabricks.id
Throw-WhenError -output $metastoreBearerToken

$metastoreDatabricksToken = ConvertTo-DatabricksPersonalAccessToken `
  -workspaceUrl $metastoreDatabricks.workspaceUrl `
  -bearerToken $metastoreBearerToken
Throw-WhenError -output $metastoreDatabricksToken

[Environment]::SetEnvironmentVariable('DATABRICKS_AAD_TOKEN', $metastoreDatabricksToken)

$metastoreDatabricksUrlHttps = "https://" + $metastoreDatabricks.workspaceUrl
$output = databricks configure --host $metastoreDatabricksUrlHttps --aad-token
Throw-WhenError -output $output

Write-Host "Create Metastore. Check if the metastore exists" -ForegroundColor DarkYellow
$metastoreStorageAccountEndpoint = $metastoreStorageAccount.primaryEndpoints.dfs.TrimStart("https://")
$metastoreContainerEndpoint = "abfss://${metastoreContainerName}@${metastoreStorageAccountEndpoint}meta"

$metastores = databricks unity-catalog metastores list | ConvertFrom-Json
Throw-WhenError -output $metastores

if ($metastores.metastores.name -notcontains $metastoreName) {
  Write-Host "Metastore does NOT exist. Creating..."
  $metastore = databricks unity-catalog metastores create `
    --name $metastoreName `
    --region $location `
    --storage-root $metastoreContainerEndpoint `
    | ConvertFrom-Json
  Throw-WhenError -output $metastore
  $metastoreId = $metastore.metastore_id
} else {
  Write-Host "Metastore ${metastoreName} exists. Getting id..."
  $metastoreId = ($metastores.metastores | Where-Object { $_.name -eq $metastoreName }).metastore_id
}

Write-Host "Assign workspace to metastore. Check if the workspace is assigned to the metastore first." -ForegroundColor DarkYellow
$assignment = databricks unity-catalog metastores get-assignment

if ($assignment.Contains("METASTORE_DOES_NOT_EXIST")) {
  Write-Host "Workspace is NOT assigned correctly. Assigning..."
  $output = databricks unity-catalog metastores assign `
    --workspace-id $metastoreDatabricks.workspaceId `
    --metastore-id $metastoreId
  Throw-WhenError -output $output
} else {
  Write-Host "Workspace is assigned correctly"
}

Write-Host "Add storage credential for access connector for workspace. Check if the workspace contains the storage credential." -ForegroundColor DarkYellow
# For the metastore to have access to the access connector, a credential must be added to the workspace,
# and then the metastore can grab this credential and use it
$storageCredentials = databricks unity-catalog storage-credentials list | ConvertFrom-Json
Throw-WhenError -output $storageCredentials

if ($storageCredentials.storage_credentials.name -contains $metastoreAccessConnectorName) {
  Write-Host "Does contain. Geting credential id..."
  $accessConnectorCredentialId = ($storageCredentials.storage_credentials | Where-Object { $_.name -eq $metastoreAccessConnectorName }).id
} else {
  Write-Host "Does NOT contain. Creating and get id..."
  $accessConnectorCredential = databricks unity-catalog storage-credentials create `
    --name $metastoreAccessConnectorName `
    --az-mi-access-connector-id $metastoreAccessConnector.id `
    | ConvertFrom-Json
    Throw-WhenError -output $accessConnectorCredential
  $accessConnectorCredentialId = $accessConnectorCredential.id
}

Write-Host "Add/update storage credential to metastore" -ForegroundColor DarkYellow
$output = databricks unity-catalog metastores update `
  --id $metastoreId `
  --storage-root-credential-id $accessConnectorCredentialId
Throw-WhenError -output $output

Write-Host "Create Catalog '$metastoreCatalogName'. Check if exists" -ForegroundColor DarkYellow
$catalogs = databricks unity-catalog catalogs list | ConvertFrom-Json

if ($catalogs.catalogs.name -contains $metastoreCatalogName) {
  Write-Host "Catalog exists"
} else {
  Write-Host "Catalog does not exist. Creating..."
  $output = databricks unity-catalog catalogs create `
  --name $metastoreCatalogName
  Throw-WhenError -output $output
}