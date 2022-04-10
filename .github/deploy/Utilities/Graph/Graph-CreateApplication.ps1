# Graph equivalent of az ad app create
function Graph-CreateApplication {

    param (
      [Parameter(Mandatory=$true)]
      [string]
      $displayName
    )

    return Graph-Rest -method "post" -url "applications" -body @{displayName=$displayName}
}
