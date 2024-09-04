Write-Host "Setting up pyodbc driver" -ForegroundColor DarkYellow
databricks fs mkdir dbfs:/databricks/drivers

$odbcSaveLocation = "$PSScriptRoot/../drivers/msodbcsql18_18.0.1.1-1_amd64.deb"

databricks fs cp --overwrite $odbcSaveLocation dbfs:/databricks/drivers/msodbcsql18_amd64.deb

Set-DatabricksGlobalInitScript `
  -workspaceUrl $workspaceUrl `
  -bearerToken $accessToken `
  -initScriptName "pyodbc-driver" `
  -initScriptContent (Get-Content "$PSScriptRoot/../drivers/pyodbc-driver.sh" -Raw)