# Graph equivalent of az ad app create
function Graph-ListApplications {

    param (
      [Parameter(Mandatory=$true)]
      [string]
      $queryDisplayName
    )

    $apps = (Graph-Rest -url "applications").value

    $apps = $apps | Where-Object {$_.displayName -eq $queryDisplayName}

    return $apps
}
