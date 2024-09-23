function Check-AzureAccountTag {
    param (
        [string]$tag
    )

    # Step 1: Get the tags of the current Azure account
    $accountTags = Get-AzureAccountTags

    # Step 2: Check if the specified tag exists and has a non-null, non-empty, non-"false" value
    if (($accountTags -ne $null) -and ($accountTags.$tag) -and ($accountTags.$tag -notin @("", "false"))) {
        return $true
    } else {
        return $false
    }
}