## This module is for deploying all the azure cloud resources needed for the databricks lakehouse ##

# Provision resource group -----------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = module.config.permanent.rg_name
  location = module.config.location
  tags = {
    creator = module.config.tags.creator
    system  = module.config.tags.system
    service = module.config.tags.service
  }
}

# Provision Cosmos DB ----------------------------------------------------------
resource "azurerm_cosmosdb_account" "cosmos_db" {
  name                = module.config.permanent.resource_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = module.config.location
  offer_type          = "Standard"
  free_tier_enabled   = true


  consistency_policy {
    consistency_level = "Eventual"
  }
  geo_location {
    failover_priority = 0
    location          = module.config.location
  }
  tags = {
    creator = module.config.tags.creator
    system  = module.config.tags.system
    service = module.config.tags.service
  }
  depends_on = [azurerm_resource_group.rg]
}