Write-Host "  Install SqlServer (Invoke-SqlCmd)" -ForegroundColor DarkYellow
Install-Module -Name SqlServer -Force

Write-Host "  Generating database user" -ForegroundColor DarkYellow

$sqlServerInstance = $databaseServerName + ".database.windows.net"

$dbUserPassword = Generate-Password
$dbUserName = "DatabricksUser"
$ReadRights = $true
$WriteRights = $true
$CreateRights = $true
$ExecRights = $true


$variables =
  "Username=$($dbUserName)",
  "Password=$($dbUserPassword)",
  "ReadRights=$($ReadRights)",
  "WriteRights=$($WriteRights)",
  "CreateRights=$($CreateRights)",
  "ExecRights=$($ExecRights)"

Write-Host "   Creating database user for user: $($dbUserName)" -ForegroundColor DarkYellow
Invoke-Sqlcmd `
  -ServerInstance $sqlServerInstance `
  -Database $deliveryDatabase `
  -Username $sqlServerAdminUser `
  -Password $sqlServerAdminPassword `
  -InputFile $PSScriptRoot/sql/createduser.sql `
  -Variable $variables

