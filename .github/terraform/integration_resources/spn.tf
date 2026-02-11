
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

resource "azuread_application_password" "captain" {
  application_id = azuread_application.captain.id
}

resource "time_sleep" "wait_for_spn" {
  depends_on      = [azuread_service_principal.captain]
  create_duration = "30s"
}

# Data plane contributor for cosmos
# https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/reference-data-plane-security
# Look up the built-in Data Contributor role under this account
data "azurerm_cosmosdb_sql_role_definition" "data_contributor" {
  resource_group_name = data.azurerm_resource_group.permanent.name
  account_name        = data.azurerm_cosmosdb_account.cosmos.name
  role_definition_id  = "00000000-0000-0000-0000-000000000002"
  depends_on = [time_sleep.wait_for_spn  ]
}

resource "azurerm_cosmosdb_sql_role_assignment" "cosmos_spn_access" {
  resource_group_name = data.azurerm_resource_group.permanent.name
  account_name        = data.azurerm_cosmosdb_account.cosmos.name
  role_definition_id  = data.azurerm_cosmosdb_sql_role_definition.data_contributor.id
  principal_id        = azuread_service_principal.captain.object_id
  scope               = data.azurerm_cosmosdb_account.cosmos.id
  depends_on = [time_sleep.wait_for_spn  ]
}

# Control-plane operator for cosmos
data "azurerm_role_definition" "cosmos_account_operator" {
  name  = "Cosmos DB Operator"
  scope = data.azurerm_cosmosdb_account.cosmos.id
}

resource "azurerm_role_assignment" "captain_cosmos_account_operator" {
  scope              = data.azurerm_cosmosdb_account.cosmos.id
  role_definition_id = data.azurerm_role_definition.cosmos_account_operator.id
  principal_id       = azuread_service_principal.captain.object_id
  depends_on = [time_sleep.wait_for_spn  ]
}


# # Set the cicd spn storage account role needed for eventhub capture blob  -------
# resource "azurerm_role_assignment" "cicd_spn" {
#   scope                = azurerm_storage_account.storage_account.id
#   role_definition_name = "Storage Blob Data Contributor"
#   principal_id         = data.azuread_service_principal.cicd_spn.object_id
#   depends_on = [
#     azurerm_databricks_access_connector.ext_access_connector,
#     azurerm_storage_account.storage_account
#   ]
# }
