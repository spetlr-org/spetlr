resource "azurerm_storage_blob" "msodbcsql18" {
  name                   = "${module.config.integration.init_drivers_folder}/${module.config.integration.pyodbc_driver_name}"
  storage_account_name   = module.config.integration.resource_name
  storage_container_name = module.config.integration.init_container_name
  type                   = "Block"
  source                 = "${path.cwd}/.github/deploy/drivers/msodbcsql18_18.0.1.1-1_amd64.deb"
}

resource "databricks_global_init_script" "pyodbc_init_script" {
  provider       = databricks.workspace
  name           = "pyodbc-driver"
  enabled        = true
  content_base64 = base64encode(<<-EOT
    #!/bin/bash
    sudo apt-get update
    sudo apt-get -f install -y
    sudo ACCEPT_EULA=Y apt-get install -q -y ${databricks_volume.init.volume_path}/${module.config.integration.pyodbc_driver_name}
    ldd /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.0.so.1.1
    echo "[ODBC Driver 18 for SQL Server]" | sudo tee -a /etc/odbcinst.ini
    echo "Description=Microsoft ODBC Driver 18 for SQL Server" | sudo tee -a /etc/odbcinst.ini
    echo "Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.0.so.1.1" | sudo tee -a /etc/odbcinst.ini
    echo 'export PATH="$PATH:/opt/microsoft/msodbcsql18/bin"' | sudo tee -a ~/.bashrc
    source ~/.bashrc
    EOT
  )
}