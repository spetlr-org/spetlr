
Write-Host "Delete every captain spn where the corresponding resource group does not exists."

$captains = az ad app list --query "[?notes != null && contains(notes, 'githubspetlr')]" --output json | ConvertFrom-Json

foreach ($spn in $captains) {
    # Check if the workspace exists using the az resource show command
    az group show --ids $spn.notes --output none

    # If the groups does not exist, delete the application
    if (-not $?) {
        Write-Host "Did not find corresponding group named $($spn.notes). Deleting application $($rg.id)."
        az ad app delete --id $spn.id
    }
}
