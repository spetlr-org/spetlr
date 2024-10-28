terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.55.0"
    }

    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.7.0"
    }
  }
  backend "azurerm" {
    use_azuread_auth     = true
    resource_group_name  = "Terraform-State-Stoarge"
    storage_account_name = "spetlrtfstate"
    container_name       = "tfstate"
    key                  = "metastore.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.db_account_id
}

provider "databricks" {
  alias = "ws"
  host  = data.azurerm_databricks_workspace.admin.workspace_url
}

module "config" {
  source = "../modules/config"
}
