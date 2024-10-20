## This module is to create the unity catalog 3-levels namespace components (catalog.schema.volume) ##
## Data componenets like tables and views will not be created here. as they will be handled during ETL ##

data "azurerm_storage_container" "init" {
  name                 = module.config.integration.init_container_name
  storage_account_name = module.config.integration.resource_name
}

# Create catalog for catalog data ---------------------------------------------------------------------------------
resource "databricks_catalog" "catalog" {
  provider = databricks.workspace

  name           = module.config.integration.resource_name
  comment        = "Catalog to encapsulate all catalog data schema"
  isolation_mode = "ISOLATED"
  # force_destroy  = true
  storage_root = databricks_external_location.catalog.url
  owner        = databricks_group.catalog_users.display_name
  depends_on = [
    databricks_external_location.catalog,
    data.databricks_group.db_metastore_admin_group
  ]
}

resource "databricks_workspace_binding" "bind_ws" {
  provider = databricks.workspace

  securable_name = databricks_catalog.catalog.name
  workspace_id   = data.azurerm_databricks_workspace.db_workspace.workspace_id
  binding_type   = "BINDING_TYPE_READ_WRITE"
}

resource "databricks_schema" "volumes" {
  provider = databricks.workspace

  catalog_name = databricks_catalog.catalog.name
  name         = "volumes"
  comment      = "this schema is for all volumes"
  properties = {
    kind = "various"
  }
  owner = databricks_group.catalog_users.display_name
  depends_on = [
    databricks_catalog.catalog,
    databricks_grants.catalog
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
  owner            = databricks_group.catalog_users.display_name
  depends_on = [
    databricks_schema.volumes,
  ]
}

resource "databricks_volume" "init" {
  provider = databricks.workspace

  name             = data.azurerm_storage_container.init.name
  catalog_name     = databricks_catalog.catalog.name
  schema_name      = databricks_schema.volumes.name
  volume_type      = "EXTERNAL"
  storage_location = databricks_external_location.init.url
  comment          = "External volume to store init driver files"
  owner            = databricks_group.catalog_users.display_name
  depends_on = [
    databricks_schema.volumes,
  ]
}

locals {
  init_vol_path = "/Volumes/${databricks_catalog.catalog.name}/volumes/${databricks_volume.init.name}"
}
