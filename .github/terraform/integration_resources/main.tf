terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.7.0"
    }
  }
  backend "local" {
    path = "integration_resources.tfstate"
  }
}

provider "azurerm" {
  features {}
}

variable "uniqueRunId" {}

module "config" {
  source      = "../modules/config"
  uniqueRunId = var.uniqueRunId
}
