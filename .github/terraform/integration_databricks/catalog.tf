## This module is to create the unity catalog 3-levels namespace components (catalog.schema.volume) ##
## Data componenets like tables and views will not be created here. as they will be handled during ETL ##

# Create catalog for catalog data ---------------------------------------------------------------------------------
resource "databricks_catalog" "catalog" {
  provider       = databricks.workspace
  name           = module.config.integration.catalog_container_name
  comment        = "Catalog to encapsulate all catalog data schema"
  isolation_mode = "ISOLATED"
  force_destroy  = true
  storage_root   = databricks_external_location.catalog.url
  owner = data.databricks_group.db_metastore_admin_group.display_name
  depends_on     = [
    databricks_external_location.catalog,
    data.databricks_group.db_metastore_admin_group
  ]
}

# Create catalog for capture data, schema, volume -----------------------------------------------------------------
resource "databricks_catalog" "capture" {
  provider       = databricks.workspace
  name           = module.config.integration.capture_container_name
  comment        = "Catalog to encapsulate all capture data schema"
  isolation_mode = "ISOLATED"
  force_destroy  = true
  storage_root   = databricks_external_location.capture.url
  owner = data.databricks_group.db_metastore_admin_group.display_name
  depends_on     = [
    databricks_external_location.capture,
    data.databricks_group.db_metastore_admin_group
  ]
}

resource "databricks_schema" "capture" {
  provider     = databricks.workspace
  catalog_name = databricks_catalog.capture.name
  name         = "${module.config.integration.capture_container_name}_schema"
  comment      = "this schema is for the capture volume"
  properties   = {
    kind = "various"
  }
  owner        = data.databricks_group.db_metastore_admin_group.display_name
  storage_root = "${databricks_external_location.capture.url}capture_schema/"
  force_destroy = true
  depends_on   = [
    databricks_catalog.capture,
    databricks_grants.capture
  ]
}

resource "databricks_volume" "capture" {
  provider         = databricks.workspace
  name             = module.config.integration.eventhub_name
  catalog_name     = databricks_catalog.capture.name
  schema_name      = databricks_schema.capture.name
  volume_type      = "EXTERNAL"
  storage_location = "${databricks_external_location.capture.url}${module.config.integration.resource_name}/${module.config.integration.eventhub_name}/"
  comment          = "External volume to store init driver files"
  owner            = data.databricks_group.db_metastore_admin_group.display_name
  depends_on       = [
    databricks_schema.capture,
  ]
}

# Create catalog, schema, volume for init data --------------------------------------------------------------------
resource "databricks_catalog" "init" {
  provider       = databricks.workspace
  name           = module.config.integration.init_container_name
  comment        = "Catalog to encapsulate all init data schema"
  isolation_mode = "ISOLATED"
  force_destroy  = true
  storage_root   = databricks_external_location.init.url
  owner = data.databricks_group.db_metastore_admin_group.display_name
  depends_on     = [
    databricks_external_location.init,
    data.databricks_group.db_metastore_admin_group
  ]
}

resource "databricks_schema" "init" {
  provider     = databricks.workspace
  catalog_name = databricks_catalog.init.name
  name         = "${module.config.integration.init_container_name}_schema"
  comment      = "this schema is for the init volume"
  properties   = {
    kind = "various"
  }
  owner        = data.databricks_group.db_metastore_admin_group.display_name
  storage_root = "${databricks_external_location.init.url}init_schema/"
  force_destroy = true
  depends_on   = [
    databricks_catalog.init,
    databricks_grants.init
  ]
}

resource "databricks_volume" "init" {
  provider         = databricks.workspace
  name             = module.config.integration.init_drivers_folder
  catalog_name     = databricks_catalog.init.name
  schema_name      = databricks_schema.init.name
  volume_type      = "EXTERNAL"
  storage_location = "${databricks_external_location.init.url}${module.config.integration.init_drivers_folder}/"
  comment          = "External volume to store init driver files"
  owner            = data.databricks_group.db_metastore_admin_group.display_name
  depends_on       = [
    databricks_schema.init,
  ]
}