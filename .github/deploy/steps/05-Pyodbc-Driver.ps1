Write-Host "Setting up pyodbc driver" -ForegroundColor DarkYellow
dbfs mkdirs dbfs:/databricks/drivers

$odbcSource='https://packages.microsoft.com/ubuntu/20.04/prod/pool/main/m/msodbcsql17/msodbcsql17_17.7.2.1-1_amd64.deb'
$odbcSaveLocation='./drivers/msodbcsql17_17.7.2.1-1_amd64.deb'

Invoke-WebRequest -Uri $odbcSource -OutFile $odbcSaveLocation

dbfs cp --overwrite $odbcSaveLocation dbfs:/databricks/drivers/msodbcsql17_amd64.deb

Set-DatabricksGlobalInitScript `
  -workspaceUrl $workspaceUrl `
  -bearerToken $accessToken `
  -initScriptName "pyodbc-driver" `
  -initScriptContent (Get-Content ./pyodbc-driver.sh -Raw)