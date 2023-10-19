
function Set-KeyVaultSecret {
    param (
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $key,
  
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $value,
  
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $keyVaultName,

      [Parameter(Mandatory = $false)]
      [string]
      $expires
    )

    if(-not $expires){
        $expires = (Get-Date).ToUniversalTime().AddYears(5).ToString("yyyy-MM-ddTHH:mm:ssZ")
    }
  
    $output = az keyvault secret set `
                        --name=$key `
                        --value=$value `
                        --vault-name=$keyVaultName `
                        --expires $expires

    Throw-WhenError -output $output
  }