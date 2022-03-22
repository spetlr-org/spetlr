# This is the script that creates the entire deployment
# for readability it is split up into separate steps
# where we try to use meaningful names.
param (
  # atc-dataplatform doesn't use separate environments
  # see atc-snippets for more inspiration
  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $environmentName="",

  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $clientId,

  [Parameter(Mandatory=$false)]
  [securestring]
  $clientSecret,

  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $tenantId
)

# import utility functions
. "$PSScriptRoot\Utilities\all.ps1"

###############################################################################################
# Configure names and options
###############################################################################################
Write-Host "Initialize deployment" -ForegroundColor Green


. "$PSScriptRoot\steps\00-Config.ps1"

###############################################################################################
# Verify arguments
###############################################################################################
. "$PSScriptRoot\steps\01-Verify-Arguments.ps1"


###############################################################################################
# Provision resource group
###############################################################################################
. "$PSScriptRoot\steps\02-Provision-Resource-Group.ps1"

Write-Host "Ready for databricks" -ForegroundColor DarkGreen

###############################################################################################
# Provision Databricks Workspace resources
###############################################################################################
. "$PSScriptRoot\steps\03-Provision-Databricks-Workspace-Resources.ps1"

###############################################################################################
# Initialize Databricks CLI
###############################################################################################
. "$PSScriptRoot\steps\04-Initialize-Databricks.ps1"

###############################################################################################
# Install ODBC driver
###############################################################################################
. "$PSScriptRoot\steps\04-Pyodbc-Driver.ps1"

###############################################################################################
# Initialize Data Lake
###############################################################################################
. "$PSScriptRoot\steps\05-Provision-Data-Lake.ps1"

###############################################################################################
# Provision SQL and database
###############################################################################################
. "$PSScriptRoot\steps\06-Provision-SQL-Server.ps1"

###############################################################################################
# Provision database databricks user
###############################################################################################
. "$PSScriptRoot\steps\07-Provision-db-user.ps1"

###############################################################################################
# Initialize Databricks secrets
###############################################################################################
. "$PSScriptRoot\steps\99-Databricks-Secrets.ps1"
