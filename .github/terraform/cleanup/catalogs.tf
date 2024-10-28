data "databricks_catalogs" "all" {
  provider   = databricks.ws
  depends_on = [databricks_metastore_assignment.db_metastore_assign_workspace]
}

output "catalogs" {
  value = data.databricks_catalogs.all.ids
}
