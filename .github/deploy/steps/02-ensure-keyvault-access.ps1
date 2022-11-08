

if(-not $pipelineClientId){
# If this value is not set, you are a human. You probably have never been given access
# to the keyvault. To ensure that we can get SPN secrets fom the keyvaul, we set do this
  $me = Get-CurrentUser
  $output = az keyvault set-policy --name $keyVaultName --secret-permissions get set list --upn "$($me.userPrincipalName)"
  Ignore-Errors
}
