###############################################################################################
# Specify dependencies for our spark application that are not pip packages.
# Use the same structure as needed by the databricks job creation json
# $spark_dependencies = @(
#     @{
#       maven = @{
#         coordinates = "com.microsoft.azure:spark-mssql-connector_2.12_3.0:1.0.0-alpha"
#       }
#     }
# )
# Pip packages should be specified in setup.py.
###############################################################################################

$spark_dependencies = @(
# we expect to add dependencies here once we have integrations to eventhubs etc.
    @{
      maven = @{
        coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21"
      }
    }
)
