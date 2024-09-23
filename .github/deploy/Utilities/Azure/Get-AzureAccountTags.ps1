function Get-AzureAccountTags {
    # Step 1: Get the current Azure account information
    $accountInfo = az account show --output json | ConvertFrom-Json
    $currentSubscriptionId = $accountInfo.id

    # Step 2: Get the tags for the current subscription
    $tagsJson = az tag list --resource-id "/subscriptions/$currentSubscriptionId/providers/Microsoft.Resources/tags/default" --output json | ConvertFrom-Json
    return $tagsJson.properties.tags
}