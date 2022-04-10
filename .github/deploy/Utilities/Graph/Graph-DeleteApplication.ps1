# Graph equivalent of az ad app create
function Graph-DeleteApplication {
    param (
      [Parameter(Mandatory=$true)]
      [string]
      $appId
    )


    Graph-Rest -method delete -url applications/$appId

}
