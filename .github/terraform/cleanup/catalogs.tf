data "databricks_catalogs" "all" {
  provider   = databricks.ws
  depends_on = [databricks_metastore_assignment.db_metastore_assign_workspace]
}



resource "databricks_workspace_binding" "integration" {
  provider = databricks.ws

  for_each = toset([for each in data.databricks_catalogs.all.ids : each if regex("spetlr[0-9]+", each)])

  securable_name = each.value
  workspace_id   = data.azurerm_databricks_workspace.admin.workspace_id
  binding_type   = "BINDING_TYPE_READ_WRITE"
}

output "catalogs" {
  value = toset([for each in databricks_workspace_binding.integration : each.securable_name])
}
