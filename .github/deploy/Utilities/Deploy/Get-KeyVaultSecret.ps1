function Get-KeyVaultSecret {
    param (
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $key,
   
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $keyVaultName
    )
  
    $output = az keyvault secret show --name=$key --vault-name=$keyVaultName --query="value" 
    Throw-WhenError -output $output
    return $output
  }