
Write-Host "Delete every resource group that is managed by a databricks workspace that no longer exists"

# Get resource groups with managedBy attribute containing 'Microsoft.Databricks/workspaces'
$resourceGroups = az group list --query "[?managedBy != null && contains(managedBy, 'Microsoft.Databricks/workspaces')]" --output json | ConvertFrom-Json

foreach ($rg in $resourceGroups) {
    # Check if the resource exists using the az resource show command
    az resource show --ids $rg.managedBy --output none

    # If the workspace does not exist, delete the group
    if (-not $?) {
        Write-Host "Did not find managing workspace for RG $($rg.name). Deleting it."
        az group delete --name $rg.id --yes --no-wait
    }
}
