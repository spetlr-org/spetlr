terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.7.0"
    }
  }
  # backend "local" {
  #   path = "integration_resources.tfstate"
  # }
  backend "azurerm" {
    use_azuread_auth = true
    resource_group_name  = "Terraform-State-Stoarge"
    storage_account_name = "spetlrtfstate"
    container_name       = "tfstate"
    key                  = "integration_resources.tfstate"
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

module "config" {
  source = "../modules/config"
}
