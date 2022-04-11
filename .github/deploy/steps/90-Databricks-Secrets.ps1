

New-DatabricksScope -name $db_secrets_scope

# $key_names = $()
foreach ($key in $keystore) {
#   $key_names += $key.name
  databricks secrets put --scope $db_secrets_scope --key $key.name --string-value $key.key
  Write-Host "  Added key '$($key.name)'"
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

