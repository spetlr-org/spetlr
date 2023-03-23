# This step provisions all service principals.
# The following SPN are used:
#  - SpetlrGithubPipe - Not provisioned here.
#      Created in the separate script setup_deploying_SPN.ps1, this SPN is supposed to
#      act as close as possible to the the human that may want to run this pipeline.
#      Keeping this 1-1 correspondence take some effort becuase SPNs can do some things
#      that humans cannot. It is up to the developer to avoid these problems. The
#      benefit of this design philosopy is that the entire pipeline can be tested
#      locally.
#  - SpetlrDbSpn
#      This SPN is made a contributor to the temporary resource group. With this right,
#      the SPN can pull an access token to deploy jobs etc to databricks. This is not
#      possible for humans. Keeping with the design principle above, we do not use the
#      deploying SPN for this.
#  - SpetlrMountSpn
#      This SPN has only one right - access to the storage account. The secret of this
#      SPN is exposed inside databricks where mistakes or malicious behavior may expose
#      it. With this secret, an attacker must not gain the ability to deploy jobs or
#      resources, hence the restricted rights.
#
# Both the SpetlrDbSpn and the SpetlrMountSpn exists only once across multiple runs.

function Get-SpnWithSecret {
    param (
      [Parameter(Mandatory=$true)]
      [string]
      $spnName,

      [Parameter(Mandatory=$true)]
      [string]
      $keyVaultName
    )

    Write-Host "Provision Service Principal $spnName" -ForegroundColor DarkYellow


  Write-Host "Verifying $spnName SPN Registration"
  $app = Graph-GetApplication -queryDisplayName $spnName
  $useOldPassword = $true

  if ($null -eq $app)
  {
    Write-Host "Creating $spnName SPN Registration" -ForegroundColor DarkGreen
    $app = Graph-CreateApplication -displayName $spnName
    $useOldPassword = $false
  }else{
    Write-Host "$spnName App registration exists" -ForegroundColor DarkGreen
  }

  $spnObject = Graph-GetSpn -queryDisplayName $spnName
  if (-not $spnObject)
  {
    Write-Host "  Creating $spnName Service Principal" -ForegroundColor DarkYellow
    $spnObject = Graph-CreateSpn -appId $app.appId
    $useOldPassword = $false
  }else{
    Write-Host "$spnName Service Principal exists" -ForegroundColor DarkGreen
  }

  if($app.passwordCredentials.length -ne 1){
      $useOldPassword = $false
  }

  While ($useOldPassword){ # only run once, but break early
    $clientSecretPlain = az keyvault secret show `
            --name=$spnName `
            --vault-name=$keyVaultName `
            --query="value" `
            --out tsv
    if($LastExitCode -ne 0) {
      # doesn't matter. Get a new one
      Ignore-Errors
      $useOldPassword = $false
      break
    }

    #   only one password exits and we got one from the keyvault. Let's see if they match
    if($clientSecretPlain.StartsWith($app.passwordCredentials[0].hint)){
      Write-Host "  Got $spnName Secret from vault" -ForegroundColor DarkYellow
      break
    }else{
      $useOldPassword = $false
      Write-Host "  WARNING: The secret from the key vault does not match the hint" -ForegroundColor DarkYellow
      break
    }
  }

  if(-not $useOldPassword){
    #  first check if there is an old key to delete.
    foreach($credential in $app.passwordCredentials){
      Graph-AppRemovePassword -keyId $credential.keyId -appId $app.id
      Write-Host "  Deleted one old password" -ForegroundColor DarkYellow
    }

#     Graph-ListApplications -queryDisplayName $spnName

    Write-Host "  Creating $spnName Secret" -ForegroundColor DarkYellow
    $clientSecretObject = (Graph-AppAddPassword -appId $app.id)
    $clientSecretPlain = $clientSecretObject.secretText

    $output = az keyvault secret set `
        --name=$spnName `
        --value=$clientSecretPlain `
        --vault-name=$keyVaultName
    Ignore-Errors -output $output
    # the keyvault may not exist
  }

  return @{
    name = $spnName
    clientId = $app.appId
    objectId = $spnObject.id
    secretText = $clientSecretPlain
    secret = (ConvertTo-SecureString -AsPlainText $clientSecretPlain -Force)
  }
}

