terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.34.0"
    }
  }
  backend "local" {
    path = "integration_resources.tfstate"
  }
}

provider "azurerm" {
  features {}
  subscription_id = "622fe0a2-b764-4aec-9366-ec0e9bab3357"
}

variable "uniqueRunId" {}

module "config" {
  source      = "../modules/config"
  uniqueRunId = var.uniqueRunId
}
