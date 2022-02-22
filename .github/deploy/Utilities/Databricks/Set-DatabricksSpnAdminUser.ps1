
function Set-DatabricksSpnAdminUser {

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

      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $workspaceUrl,

      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $resourceId
    )

    $bearerToken = Get-OAuthToken `
      -tenantId $tenantId `
      -clientId $clientId `
      -clientSecret $clientSecret

    $managementToken = Get-OAuthToken `
      -tenantId $tenantId `
      -clientId $clientId `
      -clientSecret $clientSecret `
      -scope "https://management.core.windows.net/"

    # Calling any Azure Databricks API endpoint with a SPN management token and the resource ID
    # Will automatically add the SPN as an admin user in Databricks
    # See https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#admin-user-login

    $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
    $headers.Add("Authorization", "Bearer $bearerToken")
    $headers.Add("X-Databricks-Azure-SP-Management-Token", "$managementToken")
    $headers.Add("X-Databricks-Azure-Workspace-Resource-Id", "$resourceId")

    # Comment
    $Stoploop = $false
    [int]$Retrycount = 0

    do {
      try {
        $response = Invoke-WebRequest `
        -Uri "https://$workspaceUrl/api/2.0/clusters/list-node-types" `
        -Method 'GET' `
        -Headers $headers

        if ($response.StatusCode -ne 200) {
          Write-Error $response.StatusDescription
          throw
        }

        Write-Host "Job completed"
        $Stoploop = $true
      }
      catch {
        if ($Retrycount -gt 10){
          throw
          $Stoploop = $true
        }
        else {
          Write-Host "  Databricks API failed. retry in 10 seconds" -ForegroundColor Red
          Start-Sleep -Seconds 10
          $Retrycount = $Retrycount + 1
        }
      }
    } While ($Stoploop -eq $false)


    return $bearerToken
  }
