param (
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

Write-Host "Ready for databricks connect" -ForegroundColor DarkGreen

###############################################################################################
# Initialize Databricks CLI
###############################################################################################
. "$PSScriptRoot\steps\08-Initialize-Databricks.ps1"

