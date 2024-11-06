resource "databricks_service_principal" "captain" {
  provider = databricks.account

  application_id = data.azuread_service_principal.captain.client_id
  display_name   = module.config.integration.captain.display_name
  depends_on = [
    data.azuread_service_principal.captain
  ]
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

# Grant the CiCd pipe the role to run-as captain
resource "databricks_access_control_rule_set" "use_captain" {
  provider = databricks.account

  name = "accounts/${var.db_account_id}/servicePrincipals/${databricks_service_principal.captain.application_id}/ruleSets/default"

  grant_rules {
    principals = [data.databricks_service_principal.cicd_spn.acl_principal_id]
    role       = "roles/servicePrincipal.user"
  }
}

resource "time_sleep" "databricks_access_control_rule_set_use_captain" {
  create_duration = "7s"
  depends_on = [
    databricks_access_control_rule_set.use_captain,
  ]
}
