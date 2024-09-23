
Write-Host "  Generating database user" -ForegroundColor DarkYellow

$sqlServerInstance = $databaseServerName + ".database.windows.net"

$dbUserPassword = Generate-Password
$dbUserName = "DatabricksUser"
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


Write-Host "   Creating database user for user: $($dbUserName)" -ForegroundColor DarkYellow
$output = Invoke-Sqlcmd `
  -ServerInstance $sqlServerInstance `
  -Database $deliveryDatabase `
  -Username $sqlServerAdminUser `
  -Password $sqlServerAdminPassword `
  -InputFile $sqlSourceDir/createduser.sql `
  -Variable $variables
Throw-WhenError -output $output


# Insert SQL credentials to secrets
$secrets.addSecret("SqlServer--DatabricksUser", $dbUserName)
$secrets.addSecret("SqlServer--DatabricksUserPassword", $dbUserPassword)


