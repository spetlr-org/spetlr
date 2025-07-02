# # Provision sql ----------------------------------------------------------
# resource "azurerm_mssql_server" "sql" {
#   name                         = module.config.integration.resource_name
#   resource_group_name          = azurerm_resource_group.rg.name
#   location                     = module.config.location
#   version                      = "12.0"
#   administrator_login          = "missadministrator"
#   administrator_login_password = "thisIsKat11"
#   minimum_tls_version          = "1.2"
#
#   azuread_administrator {
#     azuread_authentication_only = true
#     login_username              = module.config.integration.captain.display_name
#     object_id                   = azuread_service_principal.captain.object_id
#   }
#
#   # TODO: maybe missing firewall rules
# }
#
# resource "azurerm_mssql_database" "sql" {
#   name      = "spetlr"
#   server_id = azurerm_mssql_server.sql.id
# }
