
resource "databricks_service_principal_secret" "cicd" {
  service_principal_id = data.databricks_service_principal.cicd_spn.application_id
}

output "Spn_id" {
  value = data.databricks_service_principal.cicd_spn.application_id
}

output "Spn_token" {
  value     = databricks_service_principal_secret.cicd.secret
  sensitive = true
}


