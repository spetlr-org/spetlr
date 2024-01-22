Write-Host "Setting up pyodbc driver" -ForegroundColor DarkYellow
dbfs mkdirs dbfs:/databricks/drivers

$odbcSourceUbuntu204 = "https://packages.microsoft.com/ubuntu/20.04/prod/pool/main/m/msodbcsql18/msodbcsql18_18.3.2.1-1_amd64.deb"
$odbcSaveLocationUbuntu204 = "$PSScriptRoot/../drivers/msodbcsql18_18.3.2.1-1_amd64_ubuntu_20_04.deb"
$odbcDbSaveLocationUbuntu204 = "dbfs:/databricks/drivers/msodbcsql18_amd64_ubuntu_20_04.deb"

Invoke-WebRequest -Uri $odbcSourceUbuntu204 -OutFile $odbcSaveLocationUbuntu204

if (-Not (Test-Path $odbcSaveLocationUbuntu204)) {
  throw "msodbcsql18 for ubuntu 20.04 was not succesfully downloaded."
}

dbfs cp --overwrite $odbcSaveLocationUbuntu204 $odbcDbSaveLocationUbuntu204

$odbcSourceUbuntu224 = "https://packages.microsoft.com/ubuntu/22.04/prod/pool/main/m/msodbcsql18/msodbcsql18_18.3.2.1-1_amd64.deb"
$odbcSaveLocationUbuntu224 = "$PSScriptRoot/../drivers/msodbcsql18_18.3.2.1-1_amd64_ubuntu_22_04.deb"
$odbcDbSaveLocationUbuntu224 = "dbfs:/databricks/drivers/msodbcsql18_amd64_ubuntu_22_04.deb"

Invoke-WebRequest -Uri $odbcSourceUbuntu224 -OutFile $odbcSaveLocationUbuntu224

if (-Not (Test-Path $odbcSaveLocationUbuntu224)) {
  throw "msodbcsql18 for ubuntu 22.04 was not succesfully downloaded."
}

dbfs cp --overwrite $odbcSaveLocationUbuntu224 $odbcDbSaveLocationUbuntu224

Set-DatabricksGlobalInitScript `
  -workspaceUrl $workspaceUrl `
  -bearerToken $accessToken `
  -initScriptName "pyodbc-driver" `
  -initScriptContent (Get-Content "$PSScriptRoot/../drivers/pyodbc-driver.sh" -Raw)