## This module is responsible for creating the necessary resources for ##
## Databricks account and workspace access control ##

# Access control for metastore and workspace ---------------------------------------------------------------

## Assign the workspace to the created Metastore
resource "databricks_metastore_assignment" "db_metastore_assign_workspace" {
  provider = databricks.account

  metastore_id         = data.databricks_metastore.db_metastore.id
  workspace_id         = data.azurerm_databricks_workspace.db_workspace.workspace_id
  default_catalog_name = module.config.integration.resource_name


  depends_on = [
    data.databricks_metastore.db_metastore,
    data.azurerm_databricks_workspace.db_workspace
  ]
}



data "databricks_group" "db_metastore_admin_group" {
  provider = databricks.account

  display_name = module.config.permanent.metastore_admin_group_name
}

# Access control for users, groups and principals ---------------------------------------------------------

## Add metastore admin group to the workspace as the workspace admin
resource "databricks_mws_permission_assignment" "add_metastore_admin_group_to_workspace" {
  provider = databricks.account

  workspace_id = data.azurerm_databricks_workspace.db_workspace.workspace_id
  principal_id = data.databricks_group.db_metastore_admin_group.id
  permissions  = ["ADMIN"]
  depends_on = [
    data.azurerm_databricks_workspace.db_workspace,
    data.databricks_group.db_metastore_admin_group,
    databricks_metastore_assignment.db_metastore_assign_workspace
  ]
}

## Add metastore admin group to the workspace as the workspace admin
resource "databricks_mws_permission_assignment" "add_user_group_to_workspace" {
  provider = databricks.account

  workspace_id = data.azurerm_databricks_workspace.db_workspace.workspace_id
  principal_id = databricks_group.catalog_users.id
  permissions  = ["ADMIN"]
  depends_on = [
    databricks_metastore_assignment.db_metastore_assign_workspace
  ]
}

resource "time_sleep" "wait_for_groups_sync" {
  create_duration = "7s"
  depends_on = [
    databricks_mws_permission_assignment.add_metastore_admin_group_to_workspace,
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
  #  force_destroy  = true
  #  force_update   = true
  #  isolation_mode = "ISOLATION_MODE_ISOLATED"
  comment = "Datrabricks external storage credentials"
  depends_on = [
    databricks_metastore_assignment.db_metastore_assign_workspace,
    databricks_mws_permission_assignment.add_metastore_admin_group_to_workspace
  ]
}

resource "databricks_grants" "ex_creds" {
  provider           = databricks.workspace
  storage_credential = databricks_storage_credential.ex_storage_cred.id
  grant {
    principal  = databricks_group.catalog_users.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

## Create extrenal location and grant privilages for catalog data storage ---------------
resource "databricks_external_location" "catalog" {
  provider = databricks.workspace
  name     = "${module.config.integration.catalog_container_name}${var.uniqueRunId}"
  url = join(
    "",
    [
      "abfss://${module.config.integration.catalog_container_name}",
      "@${module.config.integration.resource_name}.dfs.core.windows.net/"
    ]
  )
  credential_name = databricks_storage_credential.ex_storage_cred.id
  comment         = "Databricks external location for catalog data"
}

resource "databricks_grants" "catalog" {
  provider = databricks.workspace

  external_location = databricks_external_location.catalog.id
  grant {
    principal  = data.databricks_group.db_metastore_admin_group.display_name
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = databricks_group.catalog_users.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

## Create extrenal location and grant privilages for capture data storage ---------------
resource "databricks_external_location" "capture" {
  provider = databricks.workspace

  name = "${module.config.integration.capture_container_name}${var.uniqueRunId}"
  url = join(
    "",
    [
      "abfss://${module.config.integration.capture_container_name}",
      "@${module.config.integration.resource_name}.dfs.core.windows.net/"
    ]
  )
  credential_name = databricks_storage_credential.ex_storage_cred.id

  comment = "Databricks external location for capture data"
  depends_on = [
    databricks_grants.ex_creds
  ]
}

resource "databricks_grants" "capture" {
  provider = databricks.workspace

  external_location = databricks_external_location.capture.id
  grant {
    principal  = databricks_group.catalog_users.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

## Create extrenal location and grant privilages for init data storage ---------------
resource "databricks_external_location" "init" {
  provider = databricks.workspace

  name = "${data.azurerm_storage_container.init.name}${var.uniqueRunId}"
  url = join(
    "",
    [
      "abfss://${data.azurerm_storage_container.init.name}",
      "@${data.azurerm_storage_container.init.storage_account_name}.dfs.core.windows.net/"
    ]
  )
  credential_name = databricks_storage_credential.ex_storage_cred.id
  comment         = "Databricks external location for init data"
  depends_on = [
    databricks_grants.ex_creds
  ]
}

resource "databricks_grants" "init" {
  provider = databricks.workspace

  external_location = databricks_external_location.init.id
  grant {
    principal  = databricks_group.catalog_users.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

# Access control for catalogs -------------------------------------------------------------------------

## Grant privilages for catalog
resource "databricks_grants" "catalog" {
  provider = databricks.workspace

  catalog = databricks_catalog.catalog.name
  grant {
    principal  = data.databricks_group.db_metastore_admin_group.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = databricks_group.catalog_users.display_name
    privileges = ["ALL_PRIVILEGES"]
  }

}

