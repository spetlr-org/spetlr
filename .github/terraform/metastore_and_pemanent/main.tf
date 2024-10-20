terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
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

module "config" {
  source = "../modules/config"
}
