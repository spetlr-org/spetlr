## This module is for deploying all the azure cloud resources needed for the databricks lakehouse ##

# Provision resource group -----------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = module.config.integration.rg_name
  location = module.config.location
  tags = {
    creator                   = module.config.tags.creator
    system                    = module.config.tags.system
    service                   = module.config.tags.service
    TemporaryTestingResources = "TemporaryTestingResources"
    uniqueRunId               = var.uniqueRunId
    associatedCaptain         = module.config.integration.captain.display_name
  }
}

# Provision storage account -----------------------------------------------------
resource "azurerm_storage_account" "storage_account" {
  name                            = module.config.integration.resource_name
  resource_group_name             = azurerm_resource_group.rg.name
  location                        = azurerm_resource_group.rg.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  allow_nested_items_to_be_public = false
  public_network_access_enabled   = true

  tags = {
    creator = module.config.tags.creator
    system  = module.config.tags.system
    service = module.config.tags.service
  }
  depends_on = [azurerm_resource_group.rg]
}

# Provision containers ---------------------------------------------------------

resource "azurerm_storage_container" "catalog" {
  name                 = module.config.integration.catalog_container_name
  storage_account_name = azurerm_storage_account.storage_account.name
}

resource "azurerm_storage_container" "capture" {
  name                 = module.config.integration.capture_container_name
  storage_account_name = azurerm_storage_account.storage_account.name
}

resource "azurerm_storage_container" "init" {
  name                 = module.config.integration.init_container_name
  storage_account_name = azurerm_storage_account.storage_account.name
}

# Provision keyvault and setting access policies -------------------------------
resource "azurerm_key_vault" "key_vault" {
  name                     = module.config.integration.resource_name
  location                 = azurerm_resource_group.rg.location
  resource_group_name      = azurerm_resource_group.rg.name
  tenant_id                = data.azurerm_client_config.current.tenant_id
  sku_name                 = "standard"
  purge_protection_enabled = false
  tags = {
    creator = module.config.tags.creator
    system  = module.config.tags.system
    service = module.config.tags.service
  }
  depends_on = [azurerm_resource_group.rg]
}

resource "azurerm_key_vault_access_policy" "spn_access" {
  key_vault_id = azurerm_key_vault.key_vault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azuread_service_principal.cicd_spn.object_id

  secret_permissions = [
    "Get",
    "Set",
    "List",
    "Delete",
    "Purge",
    "Recover",
    "Restore",
  ]
}

resource "azurerm_key_vault_access_policy" "captain_access" {
  key_vault_id = azurerm_key_vault.key_vault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azuread_service_principal.captain.object_id

  secret_permissions = [
    "Get",
    "List",
  ]
}

# Provision eventhub and its setting ------------------------------------------
resource "azurerm_eventhub_namespace" "eh" {
  location             = module.config.location
  name                 = module.config.integration.resource_name
  resource_group_name  = azurerm_resource_group.rg.name
  sku                  = "Standard"
  auto_inflate_enabled = false
}

resource "azurerm_eventhub" "eh" {
  message_retention   = 1
  name                = module.config.integration.eventhub_name
  namespace_name      = azurerm_eventhub_namespace.eh.name
  partition_count     = 1
  resource_group_name = azurerm_resource_group.rg.name

  capture_description {
    enabled             = true
    encoding            = "Avro"
    skip_empty_archives = true
    interval_in_seconds = 60
    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "fake_namespace/fake_eventhub/y={Year}/m={Month}/d={Day}/{Year}_{Month}_{Day}_{Hour}_{Minute}_{Second}_{PartitionId}"
      blob_container_name = azurerm_storage_container.capture.name
      storage_account_id  = azurerm_storage_account.storage_account.id
    }
  }
  depends_on = [
    azurerm_storage_container.capture,
    azurerm_eventhub_namespace.eh
  ]
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

# # Provision sql ----------------------------------------------------------
# resource "azurerm_mssql_server" "sql" {
#   name                         = module.config.integration.resource_name
#   resource_group_name          = azurerm_resource_group.rg.name
#   location                     = module.config.location
#   version                      = "12.0"
#   administrator_login          = "missadministrator"
#   administrator_login_password = "thisIsKat11"
#   minimum_tls_version          = "1.2"
#
#   azuread_administrator {
#     azuread_authentication_only = true
#     login_username              = module.config.integration.captain.display_name
#     object_id                   = azuread_service_principal.captain.object_id
#   }
#
#   # TODO: maybe missing firewall rules
# }
#
# resource "azurerm_mssql_database" "sql" {
#   name      = "spetlr"
#   server_id = azurerm_mssql_server.sql.id
# }

# Provision access connector and setting its role ------------------------------
resource "azurerm_databricks_access_connector" "ext_access_connector" {
  name                = module.config.integration.resource_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
  tags = {
    creator = module.config.tags.creator
    system  = module.config.tags.system
    service = module.config.tags.service
  }
  depends_on = [azurerm_resource_group.rg]
}

resource "azurerm_role_assignment" "ext_storage_role" {
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id = join(
    "",
    [azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id]
  )
  depends_on = [
    azurerm_databricks_access_connector.ext_access_connector,
    azurerm_storage_account.storage_account
  ]
}

resource "azurerm_role_assignment" "account_contributor" {
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Account Contributor"
}

# Provision log analytics workspace -------------------------------------------
resource "azurerm_log_analytics_workspace" "logs" {
  location            = module.config.location
  name                = module.config.integration.resource_name
  resource_group_name = azurerm_resource_group.rg.name
  depends_on          = [azurerm_resource_group.rg]
}

# Provision databricks service ------------------------------------------------
resource "azurerm_databricks_workspace" "db_workspace" {
  name                = module.config.integration.resource_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  tags = {
    creator = module.config.tags.creator
    system  = module.config.tags.system
    service = module.config.tags.service
  }
  depends_on = [azurerm_resource_group.rg]
}
