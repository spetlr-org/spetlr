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


resource "azurerm_key_vault_access_policy" "user_access" {
  key_vault_id = azurerm_key_vault.key_vault.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

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


# Provision databricks service ------------------------------------------------
resource "azurerm_databricks_workspace" "db_workspace" {
  name                = module.config.integration.resource_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  tags = {
    creator       = module.config.tags.creator
    system        = module.config.tags.system
    service       = module.config.tags.service
    resource_name = module.config.integration.resource_name
  }
  depends_on = [azurerm_resource_group.rg]
}
