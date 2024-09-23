## This module is for creating and managing needed service principals ##

# Provision Azure SPN for captain spn, and setting its role ---------------------
resource "azuread_application" "captain" {
  display_name = module.config.integration.captain.display_name
  notes        = module.config.integration.rg_name
  owners       = [data.azuread_service_principal.cicd_spn.object_id]
}

resource "azuread_service_principal" "captain" {
  client_id                    = azuread_application.captain.client_id
  app_role_assignment_required = false
  owners                       = [data.azuread_service_principal.cicd_spn.object_id]
}

resource "azuread_service_principal_password" "captain" {
  service_principal_id = azuread_service_principal.captain.object_id
}

resource "azurerm_role_assignment" "captain" {
  scope                = data.azurerm_subscription.primary.id
  role_definition_name = "Contributor"
  principal_id         = azuread_service_principal.captain.id
}

# Set the cicd spn storage account role needed for eventhub capture blob  -------
resource "azurerm_role_assignment" "cicd_spn" {
  scope                = azurerm_storage_account.storage_account.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azuread_service_principal.cicd_spn.object_id
  depends_on = [
    azurerm_databricks_access_connector.ext_access_connector,
    azurerm_storage_account.storage_account
    ]
}