function Convert-Safe-FromJson {

    param (
      [Parameter(Mandatory=$true)]
      [System.Object]
      $text,
  
      [Parameter(Mandatory=$false)]
      [int]
      $depth = 0
    )
  
    Throw-WhenError -output $text
  
    $raw_text = $text | Out-String
    $error_var = $null
  
    try {
      if ($depth -gt 0) {
        $output = $raw_text | ConvertFrom-Json -ErrorVariable error_var -Depth $depth
      }
      else {
        $output = $raw_text | ConvertFrom-Json -ErrorVariable error_var
      }
    }
    catch {
      $error_var = "exception"
    }
  
    if ($error_var) {
      $script_name = $MyInvocation.ScriptName
      $line_number = $MyInvocation.ScriptLineNumber
      Write-Host "Calling script: $script_name"
      Write-Host "Calling line number: $line_number"
      Write-Host "JSON text:"
      Write-Host ""
      Write-Host $raw_text
      Write-Host ""
      throw "Failed to convert JSON"
    }
  
    return $output
  }
  