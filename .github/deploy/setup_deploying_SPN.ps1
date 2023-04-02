########################################################
#  Setting up a SPN for the pipeline
########################################################

## Step 1. Ensure correct subscription
#az login
#az account list
## find the corect one
#az account set --subscription $mySpetlrAzureSubscription
Write-Host "Check correct subsctiption is selected."
$account = az account show | ConvertFrom-Json
Write-Host "Current subscription is $($account.name)"
if ($account.name -notmatch "ATC"){
  Write-Host "Expected subscription to match ATC" -ForegroundColor Red
  Write-Host "Please change subscription" -ForegroundColor Red
  Exit 1
}

####################################################################################
## Step 2. Create app registration
Write-Host "Check if app regisration already exists." -ForegroundColor DarkGreen
$appRegName = "SpetlrGithubPipe"
$appId = az ad app list `
  --display-name $appRegName `
  --query [-1].appId `
  --out tsv

if ($null -eq $appId)
{
  Write-Host "Creating SPN Registration" -ForegroundColor DarkGreen
  $appId = az ad app create --display-name $appRegName `
      --query appId `
      --out tsv

  Write-Host "  Creating Service Principal" -ForegroundColor DarkYellow
  $newSpnId = az ad sp create --id $appId
}else{
    Write-Host "App Registration exists." -ForegroundColor DarkGreen

}

####################################################################################
# Secrets are regenerated

Write-Host "  Generating SPN secret (Client App ID: $appId)" -ForegroundColor DarkYellow
$clientSecret = az ad app credential reset --id $appId --query password --out tsv
$resourceId = az ad sp show --id $appId --query id --out tsv
$tenantId = (az account show | ConvertFrom-Json).tenantId

####################################################################################
# It is not clear to me why this is needed.
Write-Host "Granting Admin Consent"
az ad app permission admin-consent --id $appId

#####################################################################################
Write-Host "Adding Owner rights. Needed to deploy resources." -ForegroundColor DarkGreen
az role assignment create `
  --assignee $appId `
  --role "Owner" `
  --subscription $account.id `
  --scope "/subscriptions/$($account.id)"

#######################################################################################
Write-Host "Adding Microsoft graph permissions." -ForegroundColor DarkGreen
# this is needed to be able to create other service principals for mounting

# get the permission ID that we need:
## this was a detour that did not quite turn up the needed ID
#$graph = az ad sp list --all --filter "displayName eq 'Microsoft Graph'" | ConvertFrom-Json
#$permissions = (az ad sp show --id $graph.appId | ConvertFrom-Json).oauth2PermissionScopes
#$permission = $permissions | Where-Object {$_.value -eq "Application.ReadWrite.All"}

# This is the id of Application.ReadWrite.OwnedBy
$permission_id = "18a4783c-866b-4cc7-a460-3d5e5662c884"
#$permission_id = $permission.id

az ad app permission add --id $appId --api $graph.appId --api-permission "$($permission_id)=Role"
az ad app permission grant --id $appId  --api $graph.appId --scope $account.id

# Add the Privileged Role Administrator to the spn
# This allows it to add the sql server to the role of Directory Reader
$roleId = "e8611ab8-c189-46e8-94e1-60213ab1f814"
Graph-CreateRole -principalId $resourceId  -roleDefinitionId $roleId


#######################################################################################
Write-Host "# please add these secrets to your github environment"
Write-Host "`$clientId = '$appId'"
Write-Host "`$clientSecret = '$clientSecret'"
Write-Host "`$tenantId = '$tenantId'"

Write-Host "Try logging in:"
Write-Host "az login --service-principal -u $appId -p $clientSecret --tenant $tenantId"
