

if(-not $pipelineClientId){
# If this value is not set, you are a human. The keyvault was just set up and configured
# with access for only the relevant service principals. To continue the pipeline as a
# human, we ensure that the relevant access exists.
# TODO: Set up an AD group that always has access and then manually add people to it.

  $me = Get-CurrentUser
  $output = az keyvault set-policy --name $keyVaultName --secret-permissions get set list --upn "$($me.userPrincipalName)"
  Ignore-Errors
}
