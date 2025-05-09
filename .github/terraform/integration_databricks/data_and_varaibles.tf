data "azurerm_client_config" "current" {}

data "azurerm_subscription" "primary" {}

data "azuread_service_principal" "captain" {
  display_name = module.config.integration.captain.display_name
}

data "azurerm_databricks_access_connector" "ext_access_connector" {
  name                = module.config.integration.resource_name
  resource_group_name = module.config.integration.rg_name
}

data "azurerm_key_vault" "key_vault" {
  name                = module.config.integration.resource_name
  resource_group_name = module.config.integration.rg_name
}

data "azurerm_key_vault_secret" "captain_spn_id" {
  name         = module.config.integration.captain.kv_secret_id
  key_vault_id = data.azurerm_key_vault.key_vault.id
}

data "azurerm_key_vault_secret" "captain_spn_password" {
  name         = module.config.integration.captain.kv_secret_pass
  key_vault_id = data.azurerm_key_vault.key_vault.id
}

data "azurerm_key_vault_secret" "captain_spn_tenant" {
  name         = module.config.integration.captain.kv_secret_tenant
  key_vault_id = data.azurerm_key_vault.key_vault.id
}

data "databricks_metastore" "db_metastore" {
  provider = databricks.account

  name = module.config.permanent.metastore_name
}

data "azurerm_databricks_workspace" "db_workspace" {
  name                = module.config.integration.resource_name
  resource_group_name = module.config.integration.rg_name
}

