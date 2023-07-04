
$cluster_spec = @"
{
  "cluster_name": "localdeploy",
  "spark_version": "11.3.x-scala2.12",
  "node_type_id": "Standard_L8s",
  "driver_node_type_id": "Standard_L8s",
  "autotermination_minutes": 10,
  "enable_elastic_disk": true,
  "spark_env_vars": {
    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
  },
  "spark_conf": {
    "spark.databricks.delta.preview.enabled": true,
    "spark.databricks.io.cache.enabled": true
  },
  "azure_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK_AZURE",
    "spot_bid_max_price": -1
  },
  "num_workers": 2
}
"@
$libraries = @"
[
  {
    "maven" : {
      "coordinates" : "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
    }
  },
  {
    "maven" : {
      "coordinates" : "com.microsoft.azure:spark-mssql-connector_2.12_3.0:1.0.0-alpha"
    }
  },
  {
    "maven" : {
      "coordinates" : "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.0.0-beta.3"
    }
  }
]
"@

spetlr_deploy_gp_cluster --cluster_json "$cluster_spec" --libraries_json "$libraries"

