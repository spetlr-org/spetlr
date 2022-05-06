###############################################################################################
# Provision eventhubs namespace
###############################################################################################

Write-Host "  Creating Event Hub Namespace: $ehNamespace " -ForegroundColor DarkYellow
$output = az eventhubs namespace create `
  --name $ehNamespace `
  --location $location `
  --resource-group $resourceGroupName `
  --sku 'standard' `
  --tags $resourceTags

Throw-WhenError -output $output

Write-Host "    Getting Event Hub connection-string"
$eventHubConnection = az eventhubs namespace authorization-rule keys list `
  --resource-group $resourceGroupName `
  --namespace-name $ehNamespace `
  --name RootManageSharedAccessKey `
  --query primaryConnectionString `
  --output tsv

Throw-WhenError -output $eventHubConnection


$secrets.addSecret("EventHubConnection", $eventHubConnection)


###############################################################################################
# Provision individual eventhubs
###############################################################################################

foreach ($eventHub in $eventHubConfig) {
  Write-Host "  Creating Event Hub : $($eventHub.name) " -ForegroundColor DarkYellow

  $captureFormat = "{Namespace}/{EventHub}/y={Year}/m={Month}/d={Day}/{Year}_{Month}_{Day}_{Hour}_{Minute}_{Second}_{PartitionId}"


  $output = az eventhubs eventhub create `
    --name $eventHub.name `
    --namespace-name $eventHub.namespace `
    --resource-group $resourceGroupName `
    --message-retention 7 `
    --partition-count 4 `
    --enable-capture true `
    --capture-interval 60 `
    --capture-size-limit 314572800 `
    --destination-name 'EventHubArchive.AzureBlockBlob' `
    --storage-account $dataLakeName `
    --blob-container $eventHub.captureLocation `
    --archive-name-format $captureFormat `
    --skip-empty-archives true

  Throw-WhenError -output $output

}
