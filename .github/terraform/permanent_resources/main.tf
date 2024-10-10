terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.7.0"
    }
  }
  backend "azurerm" {
    use_azuread_auth = true
    resource_group_name  = "Terraform-State-Stoarge"
    storage_account_name = "spetlrtfstate"
    container_name       = "tfstate"
    key                  = "permanent_resources.tfstate"
  }
}

provider "azurerm" {
  features {}
}

module "config" {
  source = "../modules/config"
}
