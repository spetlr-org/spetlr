resource "databricks_metastore" "unitycatalog" {
  name          = module.config.permanent.metastore_name
  owner         = databricks_group.metastore_owners.display_name
  region        = module.config.location
  force_destroy = false
}


