class DatabricksSecretsManager {
  $secrets = @{}


  [void] addSecret([string]$name, [string]$value) {
    # save for later so we can add it to databricks
    $this.secrets[$name] = @{
      value = $value
    }
  }

  [void] pushToDatabricks([string]$db_secrets_scope) {
    New-DatabricksScope -name $db_secrets_scope


    foreach ($name in $this.secrets.keys) {

      $jsonBody = @{
        scope        = $db_secrets_scope
        key          = $name
        string_value = ${this}.secrets[$name].value
      } | ConvertTo-Json
      databricks secrets put-secret --json $jsonBody
      Write-Host "  Added secret '$($name)'"
    }
  }

  [void] list() {
    foreach ($name in $this.secrets.keys) {
      Write-Host $name
    }
  }
}


