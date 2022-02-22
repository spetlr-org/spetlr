
function ConvertTo-DatabricksPersonalAccessToken {
  
    param (
    
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $workspaceUrl,
  
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $bearerToken,
  
      [Parameter(Mandatory=$false)]
      [ValidateNotNullOrEmpty()]
      [int]
      $lifetimeSeconds = 3600,

      [Parameter(Mandatory=$false)]
      [AllowEmptyString()]
      [string]
      $tokenComment = "SPN Token"
    )

    if([String]::IsNullOrWhiteSpace($tokenComment)){$tokenComment="SPN Token"}
    
    $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
    $headers.Add("Authorization", "Bearer $bearerToken")
  
    $request = @{ 
      comment = $tokenComment
    }
  
    if ($lifetimeSeconds -gt 0) {
      $request.lifetime_seconds = $lifetimeSeconds
    }
  
    $pat = Invoke-RestMethod `
      -Uri "https://$workspaceUrl/api/2.0/token/create" `
      -Method 'POST' `
      -Headers $headers `
      -Form $request
  
    return $pat.token_value
  }
  