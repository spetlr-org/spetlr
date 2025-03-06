terraform {
  required_providers {
  }
}

output "location" {
  value = "northeurope"
}

variable "uniqueRunId" {
  type    = string
  default = ""
}

output "tags" {
  value = {
    system  = "SPETLR-ORG"
    service = "Data Platform"
    creator = "Cloud Deployment"
  }
}

output "permanent" {
  value = {
    rg_name                    = "spetlr-permanent"
    resource_name              = "spetlrpermanent2"
    metastore_name             = "spetlrmetastore"
    metastore_admin_group_name = "spetlrmetastoreadmins"
    cicd_spn_name              = "SpetlrGithubPipe"
  }
}

output "integration" {
  value = {
    rg_name                = "spetlr${var.uniqueRunId}"
    resource_name          = "spetlr${var.uniqueRunId}"
    eventhub_name          = "spetlreh"
    kv_secret_db_ws_url    = "Databricks--WorkspaceUrl"
    catalog_container_name = "catalog"
    capture_container_name = "capture"
    init_container_name    = "init"
    init_drivers_folder    = "drivers"
    pyodbc_driver_name     = "msodbcsql18_amd64.deb"
    captain = {
      display_name     = "spetrl_captain_${var.uniqueRunId}"
      kv_secret_id     = "Captain--ClientId"
      kv_secret_pass   = "Captain--ClientSecret"
      kv_secret_tenant = "Captain--TenantId"
      kv_db_secret     = "Captain--DbSecret"
    }
  }
}
