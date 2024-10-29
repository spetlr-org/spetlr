data "databricks_service_principal" "cicd_spn" {
  display_name = module.config.permanent.cicd_spn_name
}

resource "databricks_service_principal_secret" "cicd" {
  service_principal_id = data.databricks_service_principal.cicd_spn.id
}

output "spn_id" {
  value = data.databricks_service_principal.cicd_spn.application_id
}

output "spn_token" {
  value     = databricks_service_principal_secret.cicd.secret
  sensitive = true
}


