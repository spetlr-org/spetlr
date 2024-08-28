Write-Host "  Connect to Databricks" -ForegroundColor DarkYellow
# [Environment]::SetEnvironmentVariable('DATABRICKS_AAD_TOKEN', $token)
# $output = databricks configure --host "https://$workspaceUrl" --token $token
# Throw-WhenError -output $output

$values.addSecret("resourceName", $resourceName)


$secrets.pushToDatabricks("secrets")
$values.pushToDatabricks("values")
