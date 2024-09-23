function Ignore-Errors {

    param (
      [string]
      $action,

      [System.Object]
      $output
    )

    if ($LastExitCode -gt 0)
    {
      $raw_text = $output | Out-String
      $script_name = $MyInvocation.ScriptName
      $line_number = $MyInvocation.ScriptLineNumber
      Write-Host "Calling script: $script_name"
      Write-Host "Calling line number: $line_number"
      Write-Host "An error was ignored here."
    }

    pwsh -Command { exit 0 }  # this NOOP is successful and resets the error codes.

  }
