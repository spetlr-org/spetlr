Write-Host "Setting up pyodbc driver" -ForegroundColor DarkYellow
dbfs mkdirs dbfs:/databricks/drivers

$odbcSource="https://packages.microsoft.com/ubuntu/20.04/prod/pool/main/m/msodbcsql18/msodbcsql18_18.2.1.1-1_amd64.deb"
$odbcSaveLocation="$PSScriptRoot/../drivers/msodbcsql18_18.2.1.1-1_amd64.deb"

Invoke-WebRequest -Uri $odbcSource -OutFile $odbcSaveLocation

if(-Not (Test-Path $odbcSaveLocation)){
  throw "msodbcsql18 was not succesfully downloaded."
}

dbfs cp --overwrite $odbcSaveLocation dbfs:/databricks/drivers/msodbcsql18_amd64.deb

Set-DatabricksGlobalInitScript `
  -workspaceUrl $workspaceUrl `
  -bearerToken $accessToken `
  -initScriptName "pyodbc-driver" `
  -initScriptContent (Get-Content "$PSScriptRoot/../drivers/pyodbc-driver.sh" -Raw)
