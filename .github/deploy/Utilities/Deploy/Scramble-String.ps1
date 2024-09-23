
function Scramble-String {

    param (
      [Parameter(Mandatory=$true)]
      [string]
      $inputString
    )
  
    $characterArray = $inputString.ToCharArray()   
    $scrambledStringArray = $characterArray | Get-Random -Count $characterArray.Length     
    $outputString = -join $scrambledStringArray
    return $outputString 
  }
  