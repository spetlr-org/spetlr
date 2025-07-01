# Provision eventhub and its setting ------------------------------------------
resource "azurerm_eventhub_namespace" "eh" {
  location             = module.config.location
  name                 = module.config.integration.resource_name
  resource_group_name  = azurerm_resource_group.rg.name
  sku                  = "Standard"
  auto_inflate_enabled = false
}

resource "azurerm_eventhub" "eh" {
  message_retention = 1
  name              = module.config.integration.eventhub_name
  namespace_id      = azurerm_eventhub_namespace.eh.id
  partition_count   = 1

  capture_description {
    enabled             = true
    encoding            = "Avro"
    skip_empty_archives = true
    interval_in_seconds = 60
    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "{Namespace}/{EventHub}/y={Year}/m={Month}/d={Day}/{Year}_{Month}_{Day}_{Hour}_{Minute}_{Second}_{PartitionId}"
      blob_container_name = azurerm_storage_container.capture.name
      storage_account_id  = azurerm_storage_account.storage_account.id
    }
  }
}

resource "azurerm_eventhub_authorization_rule" "root" {
  eventhub_name       = azurerm_eventhub.eh.name
  name                = "RootManageSharedAccessKey"
  namespace_name      = azurerm_eventhub_namespace.eh.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = true
  manage              = true
}

