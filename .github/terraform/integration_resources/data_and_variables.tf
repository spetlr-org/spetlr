
data "azurerm_client_config" "current" {}

data "azuread_client_config" "current" {}

data "azurerm_subscription" "primary" {
}

data "azuread_service_principal" "cicd_spn" {
  display_name = module.config.permanent.cicd_spn_name
}

data "azurerm_resource_group" "permanent" {
  name = module.config.permanent.rg_name
}

data "azurerm_cosmosdb_account" "cosmos" {
  name                = module.config.permanent.resource_name
  resource_group_name = data.azurerm_resource_group.permanent.name
}