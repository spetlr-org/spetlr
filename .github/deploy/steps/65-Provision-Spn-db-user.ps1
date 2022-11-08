Write-Host "  Generating database SPN user" -ForegroundColor DarkYellow

$sqlServerInstance = $databaseServerName + ".database.windows.net"

# https://learn.microsoft.com/en-us/azure/active-directory/roles/custom-assign-graph
# https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/howto-assign-access-cli
Write-Host "Giving sql server Directory Reader role...."
$spID=$(az resource list -n $databaseServerName --query [*].identity.principalId --out tsv)
Graph-CreateRole -principalId $spId -roleDefinitionId 88d8e3e3-8f55-4a1e-953a-9b9898b8876b
Ignore-Errors  # The role may already be there if we re-deploy

Write-Host "Waiting for role to settle...." -ForegroundColor DarkYellow
Start-Sleep -seconds 60 # If the Directory Reader is not there, increase te seconds here.

$dbUserName = $mountSpn.name
$ReadRights = $true
$WriteRights = $true
$CreateRights = $true
$ExecRights = $true
$CreateViewRights = $true


$variables =
  "Username=$($dbUserName)",
  "Password=$($dbUserPassword)",
  "ReadRights=$($ReadRights)",
  "WriteRights=$($WriteRights)",
  "CreateRights=$($CreateRights)",
  "ExecRights=$($ExecRights)",
  "CreateViewRights=$($CreateViewRights)"

Write-Host "   Get access token for SPN to SQL server..." -ForegroundColor DarkYellow
# From: https://docs.microsoft.com/en-us/powershell/module/sqlserver/invoke-sqlcmd?view=sqlserver-ps
$request = Invoke-RestMethod -Method POST `
-Uri "https://login.microsoftonline.com/$tenantId/oauth2/token"`
-Body @{ resource="https://database.windows.net/"; grant_type="client_credentials"; client_id=$dbSpn.clientId; client_secret=$dbSpn.secretText }`
-ContentType "application/x-www-form-urlencoded"
Throw-WhenError -output $request

$access_token = $request.access_token
Throw-WhenError -output $access_token

Write-Host "   Creating database user for SPN user: $($dbUserName)" -ForegroundColor DarkYellow
Invoke-Sqlcmd `
  -ServerInstance $sqlServerInstance `
  -Database $deliveryDatabase `
  -AccessToken $access_token `
  -InputFile "$sqlSourceDir/createAdUsers.sql" `
  -Variable $variables

Write-Host "   Creating database rights for SPN user: $($dbUserName)" -ForegroundColor DarkYellow
Invoke-Sqlcmd `
  -ServerInstance $sqlServerInstance `
  -Database $deliveryDatabase `
  -AccessToken $access_token `
  -InputFile $sqlSourceDir/giveDbRights.sql `
  -Variable $variables
