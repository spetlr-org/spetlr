
resource "databricks_group" "metastore_owners" {
  display_name = module.config.permanent.metastore_admin_group_name
}

resource "databricks_group_member" "cicd_spn" {
  group_id  = databricks_group.metastore_owners.id
  member_id = data.databricks_service_principal.cicd_spn.id
}
