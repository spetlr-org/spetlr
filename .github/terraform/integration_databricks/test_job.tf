
variable "spark_version" {
  default = "14.3.x-scala2.12"
}

data "databricks_node_type" "smallest" {
}

locals {
  git_root = data.external.git.result.root
  lib_file = one(fileset("${local.git_root}/dist/", "*.whl"))
}

resource "azurerm_storage_blob" "wheel" {
  name                   = local.lib_file
  storage_account_name   = data.azurerm_storage_container.init.storage_account_name
  storage_container_name = data.azurerm_storage_container.init.name
  type                   = "Block"
  source                 = local.lib_file
}

data "archive_file" "tests" {
  type = "zip"

  output_path = "${local.git_root}/dist/tests.zip"
  source_dir  = "${local.git_root}/tests"
}

resource "azurerm_storage_blob" "tests" {
  name                   = "tests.zip"
  storage_account_name   = data.azurerm_storage_container.init.storage_account_name
  storage_container_name = data.azurerm_storage_container.init.name
  type                   = "Block"
  source                 = data.archive_file.tests.output_path
}

resource "azurerm_storage_blob" "test_main" {
  name                   = "main.py"
  storage_account_name   = data.azurerm_storage_container.init.storage_account_name
  storage_container_name = data.azurerm_storage_container.init.name
  type                   = "Block"
  source                 = "${local.git_root}/.github/submit/main.py"
}


resource "databricks_job" "integration" {
  name        = "Integration and Unit Test Job"
  description = "This job executes the unit-tests defined in the spetlr repo."

  job_cluster {
    job_cluster_key = "cluster1"
    new_cluster {
      num_workers   = 0
      spark_version = var.spark_version
      node_type_id  = data.databricks_node_type.smallest.id
      spark_conf = {
        "spark.databricks.cluster.profile" : "singleNode",
        "spark.master" : "local[*, 4]",
        "spark.databricks.delta.preview.enabled" : "true",
        "spark.databricks.io.cache.enabled" : "true"
      }
      azure_attributes = {
        "availability" : "ON_DEMAND_AZURE",
        "first_on_demand" : 1,
        "spot_bid_max_price" : -1
      }
      custom_tags = {
        "ResourceClass" : "SingleNode"
      }
      spark_env_vars = {
        "PYSPARK_PYTHON" : "/databricks/python3/bin/python3"
      }
      data_security_mode = "SINGLE_USER"
      runtime_engine     = "STANDARD"
    }
  }


  task {
    task_key        = "a"
    job_cluster_key = "cluster1"

    spark_python_task {
      python_file = "${local.init_vol_path}/${azurerm_storage_blob.test_main.name}"
    }

    library {
      whl = "${local.init_vol_path}/${azurerm_storage_blob.wheel.name}"
    }

    # Libraries appropriate for DBR 14.3
    library {
      maven {
        coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22"
      }
    }
    library {
      maven {
        coordinates = "com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.34.0"
      }
    }
  }

  run_as {
    service_principal_name = databricks_service_principal.captain.application_id
  }
}

resource "databricks_permissions" "job_manage" {
  job_id = databricks_job.integration.id
  access_control {
    permission_level       = "CAN_MANAGE_RUN"
    service_principal_name = databricks_service_principal.captain.application_id
  }
}
