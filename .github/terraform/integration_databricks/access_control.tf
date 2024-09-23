## This module is responsible for creating the necessary resources for ##
## Databricks account and workspace access control ##

# Access control for metastore and workspace ---------------------------------------------------------------

## Assign the workspace to the created Metastore
resource "databricks_metastore_assignment" "db_metastore_assign_workspace" {
  provider      = databricks
  metastore_id  = data.databricks_metastore.db_metastore.id
  workspace_id  = data.azurerm_databricks_workspace.db_workspace.workspace_id
  depends_on    = [
    data.databricks_metastore.db_metastore,
    data.azurerm_databricks_workspace.db_workspace
  ]
}

# Access control for users, groups and principals ---------------------------------------------------------

## Add metastore admin group to the workspace as the workspace admin
resource "databricks_mws_permission_assignment" "add_metastore_admin_group_to_workspace" {
  provider      = databricks
  workspace_id  = data.azurerm_databricks_workspace.db_workspace.workspace_id
  principal_id  = data.databricks_group.db_metastore_admin_group.id
  permissions   = ["ADMIN"]
  depends_on    = [
    data.azurerm_databricks_workspace.db_workspace,
    data.databricks_group.db_metastore_admin_group,
    databricks_metastore_assignment.db_metastore_assign_workspace
  ]
}

# Access control for Extrenal location --------------------------------------------------------------------

## Create storage credential and grant privileges
resource "databricks_storage_credential" "ex_storage_cred" {
  provider = databricks.workspace
  name     = module.config.integration.resource_name
  azure_managed_identity {
    access_connector_id = data.azurerm_databricks_access_connector.ext_access_connector.id
  }
  comment    = "Datrabricks external storage credentials"
  depends_on = [
    databricks_metastore_assignment.db_metastore_assign_workspace,
    databricks_mws_permission_assignment.add_metastore_admin_group_to_workspace
  ]
}

resource "databricks_grants" "ex_creds" {
  provider           = databricks.workspace
  storage_credential = databricks_storage_credential.ex_storage_cred.id
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on = [
    databricks_storage_credential.ex_storage_cred
  ]
}

## Create extrenal location and grant privilages for catalog data storage ---------------
resource "databricks_external_location" "catalog" {
  provider        = databricks.workspace
  name            = module.config.integration.catalog_container_name
  url             = join(
    "",
    [
      "abfss://${module.config.integration.catalog_container_name}",
      "@${module.config.integration.resource_name}.dfs.core.windows.net/"
    ]
  )
  credential_name = databricks_storage_credential.ex_storage_cred.id
  comment         = "Databricks external location for catalog data"
  depends_on = [
    databricks_grants.ex_creds
  ]
}

resource "databricks_grants" "catalog" {
  provider          = databricks.workspace
  external_location = databricks_external_location.catalog.id
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on   = [
    databricks_external_location.catalog
    ]
}

## Create extrenal location and grant privilages for capture data storage ---------------
resource "databricks_external_location" "capture" {
  provider        = databricks.workspace
  name            = module.config.integration.capture_container_name
  url             = join(
    "",
    [
      "abfss://${module.config.integration.capture_container_name}",
      "@${module.config.integration.resource_name}.dfs.core.windows.net/"
    ]
  )
  credential_name = databricks_storage_credential.ex_storage_cred.id
  comment         = "Databricks external location for capture data"
  depends_on = [
    databricks_grants.ex_creds
  ]
}

resource "databricks_grants" "capture" {
  provider          = databricks.workspace
  external_location = databricks_external_location.capture.id
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on   = [
    databricks_external_location.capture
    ]
}

## Create extrenal location and grant privilages for init data storage ---------------
resource "databricks_external_location" "init" {
  provider        = databricks.workspace
  name            = module.config.integration.init_container_name
  url             = join(
    "",
    [
      "abfss://${module.config.integration.init_container_name}",
      "@${module.config.integration.resource_name}.dfs.core.windows.net/"
    ]
  )
  credential_name = databricks_storage_credential.ex_storage_cred.id
  comment         = "Databricks external location for init data"
  depends_on = [
    databricks_grants.ex_creds
  ]
}

resource "databricks_grants" "init" {
  provider          = databricks.workspace
  external_location = databricks_external_location.init.id
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on   = [
    databricks_external_location.init
    ]
}

# Access control for catalogs -------------------------------------------------------------------------

## Grant privilages for catalog
resource "databricks_grants" "catalog_data" {
  provider     = databricks.workspace
  catalog      = databricks_catalog.catalog.name
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on = [
    databricks_catalog.catalog,
    databricks_mws_permission_assignment.add_metastore_admin_group_to_workspace,
    ]
}

## Grant privilages for capture
resource "databricks_grants" "catalog_capture" {
  provider     = databricks.workspace
  catalog      = databricks_catalog.capture.name
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on = [
    databricks_catalog.capture,
    databricks_mws_permission_assignment.add_metastore_admin_group_to_workspace,
    ]
}

## Grant privilages for init
resource "databricks_grants" "catalog_init" {
  provider     = databricks.workspace
  catalog      = databricks_catalog.init.name
  grant {
    principal  = module.config.permanent.metastore_admin_group_name
    privileges = ["ALL_PRIVILEGES"]
  }
  depends_on = [
    databricks_catalog.init,
    databricks_mws_permission_assignment.add_metastore_admin_group_to_workspace,
    ]
}