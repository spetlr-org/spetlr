function Graph-CreateSpn {
      param (
      [Parameter(Mandatory=$true)]
      [string]
      $appId
    )

    $app = Graph-Rest -method "post" -url "servicePrincipals" -body @{appId=$appId}

    return $app
}
