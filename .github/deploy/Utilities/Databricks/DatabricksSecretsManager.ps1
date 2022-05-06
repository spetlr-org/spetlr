class DatabricksSecretsManager {
  $secrets = @()


  [void] addSecret([string]$name, [string]$value){
    # save for later so we can add it to databricks
    $this.secrets += @{
      name=$name
      value=$value
    }
  }

  [void] pushToDatabricks([string]$db_secrets_scope){
    New-DatabricksScope -name $db_secrets_scope


    foreach ($secret in $this.secrets) {

      databricks secrets put --scope $db_secrets_scope --key $secret.name --string-value $secret.value
      Write-Host "  Added secret '$($secret.name)'"
    }

# $existing_keys = Convert-Safe-FromJson -text (databricks secrets list --scope $db_secrets_scope --output JSON)
# Throw-WhenError -output $existing_keys
#
# foreach($secret in $existing_keys.secrets){
#   if(-not $key_names.Contains($secret.key)){
#     databricks secrets delete --scope $db_secrets_scope --key $secret.key
#     Write-Host "  Deleted key '$($secret.key)'"
#   }
# }


  }

}


