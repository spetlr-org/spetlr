
function Get-OAuthToken
{
  param (
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $tenantId,

      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $clientId,

      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $clientSecret,

      [Parameter(Mandatory=$false)]
      [ValidateNotNullOrEmpty()]
      [string]
      $scope = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" # AzureDatabricks Resource ID
  )

  $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"

  $multipartContent = [System.Net.Http.MultipartFormDataContent]::new()
  $stringHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
  $stringHeader.Name = "grant_type"
  $StringContent = [System.Net.Http.StringContent]::new("client_credentials")
  $StringContent.Headers.ContentDisposition = $stringHeader
  $multipartContent.Add($stringContent)

  $stringHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
  $stringHeader.Name = "client_id"
  $StringContent = [System.Net.Http.StringContent]::new($clientId)
  $StringContent.Headers.ContentDisposition = $stringHeader
  $multipartContent.Add($stringContent)

  $stringHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
  $stringHeader.Name = "client_secret"
  $StringContent = [System.Net.Http.StringContent]::new($clientSecret)
  $StringContent.Headers.ContentDisposition = $stringHeader
  $multipartContent.Add($stringContent)

  $stringHeader = [System.Net.Http.Headers.ContentDispositionHeaderValue]::new("form-data")
  $stringHeader.Name = "scope"
  $StringContent = [System.Net.Http.StringContent]::new("$scope/.default")
  $StringContent.Headers.ContentDisposition = $stringHeader
  $multipartContent.Add($stringContent)

  $body = $multipartContent

  $url = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
  $response = Invoke-RestMethod $url -Method 'POST' -Headers $headers -Body $body

  Throw-WhenError -output $response

  return $response.access_token
}
