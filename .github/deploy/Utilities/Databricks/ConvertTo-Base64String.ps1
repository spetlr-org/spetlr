
function ConvertTo-Base64String {

    param (
      [Parameter(ValueFromPipeline, Mandatory=$true)]
      [string]
      $text
    )
  
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
    return [System.Convert]::ToBase64String($bytes)
  }
  