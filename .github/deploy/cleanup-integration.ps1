$integrationTag = "TemporaryTestingResources"

$resourceGroups = az group list --query "[?tags.$integrationTag == '$integrationTag'].{Name:name, Tags:tags}" --output json
$resourceGroups | ConvertFrom-Json | ForEach-Object {
    $resourceGroupName = $_.Name
    $associatedCaptain = $_.Tags.associatedCaptain

    Write-Host "Deleting Resource Group: " $resourceGroupName
    az group delete --name $resourceGroupName --yes --no-wait
    Write-Host "Deleted Resource Group: " $resourceGroupName

    $associatedCaptain = $_.Tags.associatedCaptain
    if ($associatedCaptain) {
        Write-Host "Deleting App Registration with display name: " $associatedCaptain
        $appId = az ad app list --display-name $associatedCaptain --query "[0].appId" --output tsv
        if ($appId) {
            az ad app delete --id $appId
            Write-Host "Deleted App Registration: " $associatedCaptain
        } else {
            Write-Host "No App Registration found for display name: " $associatedCaptain
        }
    } else {
        Write-Host "No associatedCaptain tag found for Resource Group: " $resourceGroupName
    }

    Write-Host "----------------------------"
}


