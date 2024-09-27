## This module is create secret scope and secrets for the Databricks workspace ##

# Secret scope for keyvault backed secrets -------------------------------------
resource "databricks_secret_scope" "keyvault_backed_scope" {
  provider = databricks.workspace
  name     = "secrets"

  keyvault_metadata {
    resource_id = data.azurerm_key_vault.key_vault.id
    dns_name    = data.azurerm_key_vault.key_vault.vault_uri
  }
  depends_on = [databricks_service_principal_role.captain]
}

# Secret scope and secrets for project genrated values -------------------------
resource "databricks_secret_scope" "values" {
  provider = databricks.workspace
  name     = "values"
}

resource "databricks_secret" "resource_name" {
  provider     = databricks.workspace
  key          = "resourceName"
  string_value = module.config.integration.resource_name
  scope        = databricks_secret_scope.values.name
}