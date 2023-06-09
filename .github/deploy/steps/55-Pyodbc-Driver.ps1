Write-Host "Setting up pyodbc driver" -ForegroundColor DarkYellow
dbfs mkdirs dbfs:/databricks/drivers

$odbcSource="https://packages.microsoft.com/ubuntu/20.04/prod/pool/main/m/msodbcsql17/msodbcsql17_17.7.2.1-1_amd64.deb"
$odbcSaveLocation="$PSScriptRoot/../drivers/msodbcsql17_17.7.2.1-1_amd64.deb"

Invoke-WebRequest -Uri $odbcSource -OutFile $odbcSaveLocation

if(-Not (Test-Path $odbcSaveLocation)){
  throw "msodbcsql17 was not succesfully downloaded."
}

dbfs cp --overwrite $odbcSaveLocation dbfs:/databricks/drivers/msodbcsql17_amd64.deb


# Use https://github.com/databricks/databricks-sdk-py/blob/main/examples/global_init_scripts/create_global_init_scripts.py


# Set-DatabricksGlobalInitScript `
#   -workspaceUrl $workspaceUrl `
#   -bearerToken $accessToken `
#   -initScriptName "pyodbc-driver" `
#   -initScriptContent (Get-Content "$PSScriptRoot/../drivers/pyodbc-driver.sh" -Raw)


python -m pyodbc_global_init.py