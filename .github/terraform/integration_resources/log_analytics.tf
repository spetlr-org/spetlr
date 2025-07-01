# Provision log analytics workspace -------------------------------------------
resource "azurerm_log_analytics_workspace" "logs" {
  location            = module.config.location
  name                = module.config.integration.resource_name
  resource_group_name = azurerm_resource_group.rg.name
  depends_on          = [azurerm_resource_group.rg]
}
