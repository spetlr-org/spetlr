
function Set-TaggedSubscription {
    param (
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $tag
    )


    # Step 1: Get all subscriptions
    $subscriptions = az account list --query "[].{subscriptionId: id, subscriptionName: name}" --output json | ConvertFrom-Json

    # Step 2: Loop through each subscription and get the tags
    foreach ($subscription in $subscriptions) {
        $subscriptionId = $subscription.subscriptionId
        $subscriptionName = $subscription.subscriptionName
        # Write-Host "Processing subscription $($subscription.subscriptionName) ($subscriptionId)"

        # we need to use the account before we can list its tags, unfortunately this makes it slow.
        az account set --subscription $subscriptionId

        # Get the tags for the current subscription
        if (Check-AzureAccountTag $tag){
            Write-Host "Setting active subscription to $($subscription.subscriptionName) ($subscriptionId)"
            return $subscriptionId
        }
    }

    # if we got to here we did not find a match
    throw "No subscription with such a tag."
  }