

# To ensure that we can get SPN secrets from the keyvault, we set the role
if($pipelineClientId){

  $output = az keyvault set-policy --name $keyVaultName --secret-permissions get set list --spn $pipelineClientId
  Ignore-Errors

}else{

  # If this value is not set, you are a human. You probably have never been given access
  # to the keyvault.
  $myid = az ad signed-in-user show --query id
  $output = az keyvault set-policy --name $keyVaultName --secret-permissions get set list --object-id "$($myid)"
  Ignore-Errors

}

# any errors means that the keyvault probably doesn't exists.
# we'll create it later and then it will be ours anyways.
