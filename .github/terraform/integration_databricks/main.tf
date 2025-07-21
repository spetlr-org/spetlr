terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.34.0"
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
  features {}
  subscription_id = "622fe0a2-b764-4aec-9366-ec0e9bab3357"
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


variable "db_account_id" {
  description = "The databricks Account Id"
}

variable "uniqueRunId" {}

module "config" {
  source      = "../modules/config"
  uniqueRunId = var.uniqueRunId
}
