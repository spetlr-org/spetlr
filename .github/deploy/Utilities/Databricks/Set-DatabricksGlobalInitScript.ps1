function Set-DatabricksGlobalInitScript {
  
    param (
    
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $workspaceUrl,
  
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $bearerToken,
  
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $initScriptName,
  
      [Parameter(Mandatory=$true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $initScriptContent
    )
    
    $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
    $headers.Add("Authorization", "Bearer $bearerToken")
    $endpoint = "https://$workspaceUrl/api/2.0/global-init-scripts"
  
    Write-Host "  Checking for existing global init script called $initScriptName"
    $response = Invoke-RestMethod `
      -Uri $endpoint `
      -Method 'GET' `
      -Headers $headers
  
    $scriptId = $null
    if ($response) {
      foreach ($item in $response.scripts) {
        if ($item.name -eq $initScriptName) {
          $scriptId = $item.script_id
        }
      }
    }
  
    if ($scriptId) {
      Write-Host "  Deleting existing global init script called $initScriptName"
      $result = Invoke-RestMethod `
        -Uri "$endpoint/$scriptId" `
        -Method Delete `
        -Headers $headers
    }
  
    $base64 = ConvertTo-Base64String $initScriptContent
    $request = @{ 
      name = $initScriptName
      script = $base64
      position = 0
      enabled = "true"
    }
    
    Write-Host "  Creating new global init script called $initScriptName"
    $result = Invoke-RestMethod `
      -Uri $endpoint `
      -Method Post `
      -Headers $headers `
      -Form $request
  
    if (!$result -or $result.script_id -eq "") {
      throw "Unable to set global init script"
    }
  }
  