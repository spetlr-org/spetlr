$eventHubConnection = az eventhubs namespace authorization-rule keys list `
  --resource-group $resourceGroupName `
  --namespace-name $ehNamespace `
  --name RootManageSharedAccessKey `
  --query primaryConnectionString `
  --output tsv

Throw-WhenError -output $eventHubConnection

Write-Host "  Saving EventHubConnection" -ForegroundColor DarkYellow
$secrets.addSecret("EventHubConnection", $eventHubConnection)
