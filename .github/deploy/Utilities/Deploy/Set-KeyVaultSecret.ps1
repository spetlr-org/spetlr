
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
      $keyVaultName
    )
  
    $output = az keyvault secret set --name=$key --value=$value --vault-name=$keyVaultName
    Throw-WhenError -output $output
  }