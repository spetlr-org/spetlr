param (
    [Parameter(Mandatory = $false)] # This should be true in final version
    [ValidateNotNullOrEmpty()]
    [string]
    $uniqueRunId="" # default should be removed in final version
)

###############################################################################################
# Get the resource group name and resource name
###############################################################################################
$resourceGroupName  = "githubspetlr${$uniqueRunId}"  # This name is also used in the Terraform section
$resourceName = "githubspetlr${$uniqueRunId}"  # This name is also used in the Terraform section

Write-Host "Resource group name is $resourceGroupName"
Write-Host "Resource name is $resourceName"
###############################################################################################
# Connect to Databricks
###############################################################################################
Write-Host "Get Databricks workspace URL" -ForegroundColor Green
$workspaceUrl = az resource show `
    --resource-group $resourceGroupName `
    --name $resourceName `
    --resource-type "Microsoft.Databricks/workspaces" `
    --query properties.workspaceUrl `
    --out tsv

$workspaceUrl = "https://$workspaceUrl"
Write-Host "Workspace URL is: $workspaceUrl" -ForegroundColor DarkYellow

# Write-Host "Get Databricks captain SPN id " -ForegroundColor Green
# $workspaceSpnId = az keyvault secret show `
#     --vault-name $resourceName `
#     --name $kvDbId `
#     --query value `
#     --out tsv

# Write-Host "Get Databricks captain SPN secret " -ForegroundColor Green
# $workspaceSpnToken = az keyvault secret show `
#     --vault-name $resourceName `
#     --name $kvDbSecret `
#     --query value `
#     --out tsv

Write-Host "Generate .databrickscfg" -ForegroundColor DarkYellow
Set-Content ~/.databrickscfg "[DEFAULT]"
Add-Content ~/.databrickscfg "host = $workspaceUrl"
# Add-Content ~/.databrickscfg "client_id = $workspaceSpnId"
# Add-Content ~/.databrickscfg "client_secret = $workspaceSpnToken"
Add-Content ~/.databrickscfg ""