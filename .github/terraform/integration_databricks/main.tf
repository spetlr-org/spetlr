terraform {
  required_providers {
    azurerm   = {
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
  host = "https://accounts.azuredatabricks.net"
  account_id = var.db_account_id
}

provider "databricks" {
  alias = "workspace"
  host  = data.azurerm_key_vault_secret.workspace_url.value
  azure_client_id = data.azurerm_key_vault_secret.captain_spn_id.value
  azure_client_secret = data.azurerm_key_vault_secret.captain_spn_password.value
  azure_tenant_id = data.azurerm_key_vault_secret.captain_spn_tenant.value
}

module "config" {
  source = "../modules/config"
}