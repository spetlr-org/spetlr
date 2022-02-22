function Throw-WhenError {

    param (
      [string]
      $action,
  
      [System.Object]
      $output
    )
  
    if ($LastExitCode -gt 0)
    {
      $raw_text = $output | Out-String
      Write-Error $raw_text
      $script_name = $MyInvocation.ScriptName
      $line_number = $MyInvocation.ScriptLineNumber
      Write-Host "Calling script: $script_name"
      Write-Host "Calling line number: $line_number"
      Write-Host ""
      Write-Host $raw_text
      Write-Host ""
      throw "Failed executing command"
    }
  }