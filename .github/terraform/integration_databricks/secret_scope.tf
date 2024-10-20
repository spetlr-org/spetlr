## This module is create secret scope and secrets for the Databricks workspace ##

# Secret scope for keyvault backed secrets -------------------------------------
resource "databricks_secret_scope" "keyvault_backed_scope" {
  provider = databricks.workspace

  name = "secrets"

  keyvault_metadata {
    resource_id = data.azurerm_key_vault.key_vault.id
    dns_name    = data.azurerm_key_vault.key_vault.vault_uri
  }

  depends_on = [
    databricks_metastore_assignment.db_metastore_assign_workspace
  ]
}

# Secret scope and secrets for project genrated values -------------------------
resource "databricks_secret_scope" "values" {
  provider = databricks.workspace

  name = "values"
  depends_on = [
    databricks_metastore_assignment.db_metastore_assign_workspace
  ]
}

resource "databricks_secret" "resource_name" {
  provider = databricks.workspace

  key          = "resourceName"
  string_value = module.config.integration.resource_name
  scope        = databricks_secret_scope.values.name
}

resource "databricks_secret_acl" "value_usr" {
  provider = databricks.workspace

  permission = "READ"
  principal  = databricks_group.catalog_users.display_name
  scope      = databricks_secret_scope.values.name
}

resource "databricks_secret_acl" "secrets_usr" {
  provider = databricks.workspace

  permission = "READ"
  principal  = databricks_group.catalog_users.display_name
  scope      = databricks_secret_scope.keyvault_backed_scope.name
}
