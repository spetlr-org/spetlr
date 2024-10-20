
resource "databricks_group" "catalog_users" {
  provider     = databricks.account
  display_name = "${module.config.integration.resource_name} users"
}

resource "databricks_group_member" "captain" {
  provider = databricks.account

  group_id  = databricks_group.catalog_users.id
  member_id = databricks_service_principal.captain.id
}

resource "databricks_group_member" "metastore" {
  provider = databricks.account

  group_id  = databricks_group.catalog_users.id
  member_id = data.databricks_group.db_metastore_admin_group.id
}

