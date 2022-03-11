
###############################################################################################
# Provision Azure SQL
###############################################################################################
Write-Host "Provision Azure SQL " -ForegroundColor DarkGreen

Write-Host "  Generating secure admin password" -ForegroundColor DarkYellow
$sqlServerAdminUser = "DataPlatformAdmin"
$sqlServerAdminPassword = Generate-Password

Write-Host "  Creating SQL server" -ForegroundColor DarkYellow
$output = az sql server create `
  --name $databaseServerName `
  --location $location `
  --resource-group $resourceGroupName `
  --admin-password """$sqlServerAdminPassword""" `
  --admin-user $sqlServerAdminUser

Throw-WhenError -output $output

Write-Host "  Configure SQL Server firewall rules" -ForegroundColor DarkYellow
$output = az sql server firewall-rule create `
  --server $databaseServerName `
  --resource-group $resourceGroupName `
  --name 'AllowAllWindowsAzureIps' `
  --start-ip-address '0.0.0.0' `
  --end-ip-address '0.0.0.0'

Throw-WhenError -output $output


#############################################################################################
# Provision SQL database
#############################################################################################
Write-Host "  Creating Database" -ForegroundColor DarkGreen
$output = az sql db create `
  --resource-group $resourceGroupName `
  --server $databaseServerName `
  --name $deliveryDatabase `
  --edition "GeneralPurpose" `
  --family "Gen5" `
  --min-capacity 0.5 `
  --capacity 1 `
  --compute-model "Serverless" `
  --zone-redundant false `
  --backup-storage-redundancy Local

Throw-WhenError -output $output

