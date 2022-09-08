function Generate-Password {

    param (
      [Parameter(Mandatory=$false)]
      [int]
      $length = 32
    )

    $password = Get-RandomCharacters -length ($length / 4) -characters 'abcdefghiklmnoprstuvwxyz'
    $password += Get-RandomCharacters -length ($length / 4) -characters 'ABCDEFGHKLMNOPRSTUVWXYZ'
    $password += Get-RandomCharacters -length ($length / 4) -characters '1234567890'
    $password += Get-RandomCharacters -length ($length / 4) -characters '_.!?[]'
    return Scramble-String $password
  }


