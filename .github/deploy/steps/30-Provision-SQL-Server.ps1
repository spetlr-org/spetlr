
###############################################################################################
# Provision Azure SQL
###############################################################################################
Write-Host "Provision Azure SQL " -ForegroundColor DarkGreen

Write-Host "  Generating secure admin password" -ForegroundColor DarkYellow
$sqlServerAdminUser = "DataPlatformAdmin"
$sqlServerAdminPassword = Generate-Password

# Add to databrick secrets
$secrets.addSecret("SqlServer--DataPlatformAdmin", $sqlServerAdminUser)
$secrets.addSecret("SqlServer--DataPlatformAdminPassword", $sqlServerAdminPassword)


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

# Add user own IP to SQL server firewall
$allowUserIp =(Invoke-WebRequest -UseBasicParsing "ifconfig.me/ip").Content.Trim()

Write-Host "     Allow user IP: $allowUserIp" -ForegroundColor DarkYellow
$output = az sql server firewall-rule create `
  --server $databaseServerName `
  --resource-group $resourceGroupName `
  --name "Allow $allowUserIp" `
  --start-ip-address $allowUserIp `
  --end-ip-address $allowUserIp

Throw-WhenError -output $output



#############################################################################################
# Provision SQL database
#############################################################################################
Write-Host "  Creating Database: $deliveryDatabase" -ForegroundColor DarkGreen
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

