## This module is to create the unity catalog 3-levels namespace components (catalog.schema.volume) ##
## Data componenets like tables and views will not be created here. as they will be handled during ETL ##

# Create catalog for catalog data ---------------------------------------------------------------------------------
resource "databricks_catalog" "catalog" {
  provider = databricks.workspace

  name           = module.config.integration.resource_name
  comment        = "Catalog to encapsulate all catalog data schema"
  isolation_mode = "ISOLATED"
  # force_destroy  = true
  storage_root = databricks_external_location.catalog.url
  owner        = data.databricks_group.db_metastore_admin_group.display_name
  depends_on = [
    databricks_external_location.catalog,
    data.databricks_group.db_metastore_admin_group
  ]
}

resource "databricks_schema" "volumes" {
  provider = databricks.workspace

  catalog_name = databricks_catalog.catalog.name
  name         = "Volumes"
  comment      = "this schema is for all volumes"
  properties = {
    kind = "various"
  }
  owner = data.databricks_group.db_metastore_admin_group.display_name
  depends_on = [
    databricks_catalog.catalog,
    databricks_grants.capture
  ]
}

resource "databricks_volume" "capture" {
  provider = databricks.workspace

  name             = module.config.integration.capture_container_name
  catalog_name     = databricks_catalog.catalog.name
  schema_name      = databricks_schema.volumes.name
  volume_type      = "EXTERNAL"
  storage_location = databricks_external_location.capture.url
  comment          = "External volume to store init driver files"
  owner            = data.databricks_group.db_metastore_admin_group.display_name
  depends_on = [
    databricks_schema.volumes,
  ]
}

resource "databricks_volume" "init" {
  provider = databricks.workspace

  name             = module.config.integration.init_drivers_folder
  catalog_name     = databricks_catalog.catalog.name
  schema_name      = databricks_schema.volumes.name
  volume_type      = "EXTERNAL"
  storage_location = databricks_external_location.init.url
  comment          = "External volume to store init driver files"
  owner            = data.databricks_group.db_metastore_admin_group.display_name
  depends_on = [
    databricks_schema.volumes,
  ]
}
