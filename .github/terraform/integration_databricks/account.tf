## This module is for managing the databricks account like metastore, groups, users, SPNs, ... ##

# Manage metastore admin groups, SPNs and members ----------------------------------------------
resource "databricks_service_principal" "captain" {
  provider = databricks.account

  application_id = data.azuread_service_principal.captain.client_id
  display_name   = module.config.integration.captain.display_name
  depends_on = [
    data.azuread_service_principal.captain
  ]
}

resource "databricks_group" "catalog_users" {
  provider     = databricks.account
  display_name = module.config.integration.resource_name
}

resource "databricks_group_member" "captain" {
  provider = databricks.account

  group_id  = databricks_group.catalog_users.id
  member_id = databricks_service_principal.captain.id
}

resource "databricks_service_principal_secret" "captain_secret" {
  provider = databricks.account

  service_principal_id = databricks_service_principal.captain.id
}

resource "azurerm_key_vault_secret" "captain_db_secret" {
  key_vault_id = data.azurerm_key_vault.key_vault.id
  name         = module.config.integration.captain.kv_db_secret
  value        = databricks_service_principal_secret.captain_secret.secret
  depends_on = [
    databricks_service_principal_secret.captain_secret,
    data.azurerm_key_vault.key_vault,
  ]
}
