function Get-RandomCharacters {

    param (
      [Parameter(Mandatory=$true)]
      [int]
      $length,
  
      [Parameter(Mandatory=$true)]
      [string]
      $characters
    )
  
    $random = 1..$length | ForEach-Object { Get-Random -Maximum $characters.length }
    $private:ofs=""
    return [String]$characters[$random]
  }
  