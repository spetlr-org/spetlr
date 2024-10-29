data "databricks_metastore" "db_metastore" {
  name = module.config.permanent.metastore_name
}

resource "databricks_metastore_assignment" "db_metastore_assign_workspace" {
  metastore_id = data.databricks_metastore.db_metastore.id
  workspace_id = data.azurerm_databricks_workspace.admin.workspace_id
}

output "workspace_url" {
  value = data.azurerm_databricks_workspace.admin.workspace_url
}
