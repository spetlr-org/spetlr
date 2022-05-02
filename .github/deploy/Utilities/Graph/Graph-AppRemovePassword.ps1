function Graph-AppRemovePassword {
    param (
      [Parameter(Mandatory=$true)]
      [string]
      $keyId,

      [Parameter(Mandatory=$true)]
      [string]
      $appId
    )


    Graph-Rest `
        -method post `
        -url applications/$appId/removePassword `
        -body @{
          keyId=$keyId
        }
}
