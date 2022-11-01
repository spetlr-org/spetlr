# Graph equivalent of az ad app create
function Graph-CreateApplication {

  param (
    [Parameter(Mandatory = $true)]
    [string]
    $displayName,
    [Parameter(Mandatory = $false)]
    [string]
    $identifierUri
  )
  if ($null -eq $identifierUri) {
    $app = Graph-Rest -method "post" -url "applications" -body @{displayName = $displayName }
  }
  else {
    $app = Graph-Rest -method "post" -url "applications" -body @{displayName = $displayName; identifierUris = @($identifierUri)}
  }

  return $app
}
