## This module is for creating keyvault secrets ##

# Create a secret for the eh connection string -------------------------
resource "azurerm_key_vault_secret" "eh_connection" {

  key_vault_id = azurerm_key_vault.key_vault.id
  name         = "EventHubConnection"
  value        = azurerm_eventhub_authorization_rule.root.primary_connection_string
  depends_on = [
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access,
    azurerm_eventhub_authorization_rule.root
  ]
}

# Create secrets for the created captain spn credentials ----------------
resource "azurerm_key_vault_secret" "captain_id" {
  key_vault_id = azurerm_key_vault.key_vault.id
  name         = module.config.integration.captain.kv_secret_id
  value        = azuread_service_principal.captain.client_id
  depends_on = [
    azuread_service_principal.captain,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

resource "azurerm_key_vault_secret" "captain_secret" {
  key_vault_id = azurerm_key_vault.key_vault.id
  name         = module.config.integration.captain.kv_secret_pass
  value        = azuread_application_password.captain.value
  depends_on = [
    azuread_application_password.captain,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

resource "azurerm_key_vault_secret" "captain_tenant_id" {

  key_vault_id = azurerm_key_vault.key_vault.id
  name         = module.config.integration.captain.kv_secret_tenant
  value        = azuread_service_principal.captain.application_tenant_id
  depends_on = [
    azuread_service_principal.captain,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

# Create secrets for the cosmos db account --------------------------------
resource "azurerm_key_vault_secret" "cosmos_key" {
  key_vault_id = azurerm_key_vault.key_vault.id
  name         = "Cosmos--AccountKey"
  value        = data.azurerm_cosmosdb_account.cosmos.primary_key
  depends_on = [
    data.azurerm_cosmosdb_account.cosmos,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

resource "azurerm_key_vault_secret" "cosmos_endpoint" {
  key_vault_id = azurerm_key_vault.key_vault.id
  name         = "Cosmos--Endpoint"
  value        = data.azurerm_cosmosdb_account.cosmos.endpoint
  depends_on = [
    data.azurerm_cosmosdb_account.cosmos,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

# Create secrets for the log analytics workspace --------------------------
resource "azurerm_key_vault_secret" "log_ws_id" {

  key_vault_id = azurerm_key_vault.key_vault.id
  name         = "LogAnalyticsWs--WorkspaceID"
  value        = azurerm_log_analytics_workspace.logs.workspace_id
  depends_on = [
    azurerm_log_analytics_workspace.logs,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

resource "azurerm_key_vault_secret" "log_ws_key1" {
  key_vault_id = azurerm_key_vault.key_vault.id
  name         = "LogAnalyticsWs--PrimaryKey"
  value        = azurerm_log_analytics_workspace.logs.primary_shared_key
  depends_on = [
    azurerm_log_analytics_workspace.logs,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}

resource "azurerm_key_vault_secret" "log_ws_key2" {
  key_vault_id = azurerm_key_vault.key_vault.id
  name         = "LogAnalyticsWs--SecondaryKey"
  value        = azurerm_log_analytics_workspace.logs.secondary_shared_key
  depends_on = [
    azurerm_log_analytics_workspace.logs,
    azurerm_key_vault.key_vault,
    azurerm_key_vault_access_policy.spn_access
  ]
}
#
# # Create a secret for the created databricks workspace url ---------------------
# resource "azurerm_key_vault_secret" "db_ws_url" {
#   name         = module.config.integration.kv_secret_db_ws_url
#   value        = "https://${azurerm_databricks_workspace.db_workspace.workspace_url}/"
#   key_vault_id = azurerm_key_vault.key_vault.id
#   depends_on = [
#     azurerm_key_vault.key_vault,
#     azurerm_databricks_workspace.db_workspace,
#     azurerm_key_vault_access_policy.spn_access
#   ]
# }
