function Graph-AppAddPassword {
    param (
      [Parameter(Mandatory=$false)]
      [string]
      $displayName="spn password",

      [Parameter(Mandatory=$true)]
      [string]
      $appId
    )


    $app = Graph-Rest `
        -method post `
        -url applications/$appId/addPassword `
        -body @{
          passwordCredential=@{
            displayName = $displayName
    #        you can add end and start date here
    #        https://docs.microsoft.com/en-us/graph/api/serviceprincipal-addpassword?view=graph-rest-1.0&tabs=http
          }
        }

#    The most important output property is secretText
    return $app
}
