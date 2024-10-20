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
variable "subscription_id" {}
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}
variable "uniqueRunId" {}

module "config" {
  source      = "../modules/config"
  uniqueRunId = var.uniqueRunId
}
