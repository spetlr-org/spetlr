function New-DatabricksScope {
    param (
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $name,

      [Parameter(Mandatory = $false)]
      [ValidateNotNullOrEmpty()]
      [string]
      $keyVaultDns,

      [Parameter(Mandatory = $false)]
      [ValidateNotNullOrEmpty()]
      [string]
      $keyVaultResourceId
    )

    $json = Convert-Safe-FromJson -text (databricks secrets list-scopes --output JSON)
    if($json.scopes){
      $scopes = $json | Select-Object -expand "scopes" | Select-Object -expand "name"
      if ($scopes -contains $name) {
        Write-Host "  The scope '$name' already exists" -ForegroundColor DarkYellow
        return
      }
    }

    Write-Host "  Create secret scope '$name'" -ForegroundColor DarkYellow
    if ($keyVaultDns -and $keyVaultResourceId) {
      databricks secrets create-scope --scope $name --scope-backend-type AZURE_KEYVAULT --resource-id $keyVaultResourceId --dns-name $keyVaultDns --initial-manage-principal users
    }
    else {
      databricks secrets create-scope --scope $name --initial-manage-principal users
    }
  }
