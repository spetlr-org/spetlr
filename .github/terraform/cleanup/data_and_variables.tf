
variable "db_account_id" {
  description = "The databricks Account Id."
}

data "azurerm_resource_group" "rg" {
  name = module.config.permanent.rg_name
}

data "azurerm_databricks_workspace" "admin" {
  name                = "spetlradmin"
  resource_group_name = data.azurerm_resource_group.rg.name
}

