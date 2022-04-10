# Graph equivalent of az ad app create
function Graph-ListSpn {

    param (
      [Parameter(Mandatory=$true)]
      [string]
      $queryDisplayName
    )

    $apps = (Graph-Rest -url "servicePrincipals").value

    if($queryDisplayName){
      return $apps | Where-Object {$_.displayName -eq $queryDisplayName}
    }

    return $apps
}
