data "databricks_service_principal" "cicd_spn" {
  display_name = module.config.permanent.cicd_spn_name
}

variable "db_account_id" {
  description = "The databricks Account Id."
}
