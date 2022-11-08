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
  $pipelineClientId,

  [Parameter(Mandatory=$false)]
  [string]
  $uniqueRunId=''
)

# import utility functions
. "$PSScriptRoot\Utilities\all.ps1"

###############################################################################################
# Execute steps in order
###############################################################################################

Get-ChildItem "$PSScriptRoot/steps" -Filter *.ps1 | Sort-Object name | Foreach-Object {
  . ("$_")
}
