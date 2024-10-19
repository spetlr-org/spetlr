terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.7.0"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
  backend "local" {
    path = "integration_databricks.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }
}
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.db_account_id
}

provider "databricks" {
  alias = "workspace"
  host  = data.azurerm_databricks_workspace.db_workspace.workspace_url
}


variable "uniqueRunId" {}

module "config" {
  source      = "../modules/config"
  uniqueRunId = var.uniqueRunId
}
