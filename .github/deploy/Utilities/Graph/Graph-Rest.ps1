function Graph-Rest {
    param (
      [Parameter(Mandatory=$false)]
      [string]
      $method="get",

      [Parameter(Mandatory=$true)]
      [string]
      $url,

      [Parameter(Mandatory=$false)]
      $body=$null
    )

    if($body){
      $body | ConvertTo-Json -Compress -Depth 100 | Set-Content body.json
      $resp = Convert-Safe-FromJson -text (az rest `
        --method $method `
        --header Content-Type=application/json `
        --url https://graph.microsoft.com/v1.0/$url `
        --body '@body.json')
      Remove-Item body.json
    }
    else{
      $resp = Convert-Safe-FromJson -text (az rest `
        --method $method `
        --url https://graph.microsoft.com/v1.0/$url )
    }

    return $resp
}
