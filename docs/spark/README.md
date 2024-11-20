
# Spark class

The `Spark` class should be utilized to start a Spark session in Spetlr.
Sessions are created using the standard `pyspark.sql.SparkSession` class.
However, it is also possible to switch to Databricks Connect for Python,
and use the `databricks.connect.DatabricksSession` class instead. This can
be useful to run unit tests and integration tests from your local machine.

The library itself is not part of the Spetlr package, and it needs to be
installed separately using pip or poetry. It is loaded by Spetlr
dynamically, if needed, and can be used instead of the SparkSession class.
Be sure to install the right version of the "databricks-connect" library
matching the Databricks runtime on your cluster, e.g. 14.3.

To use Databricks Connect with Spetlr, all you need to do is to set
the following environment variable in your operating system to "True": 

**SPETLR_DATABRICKS_CONNECT**

If the library is available, the Spark class will utilize
Databricks Connect. The library will connect to the configured cluster
automatically and spin it up if necessary.

After installing the library using pip or poetry, you can set up
Databricks Connect using e.g. the following PowerShell commands.
Just set the correct host URL to your Databricks workspace, and change
the profile name below to your liking. Databricks will let you choose
a cluster and remember it. 

``` 
$my_profile='profile1',
$host_url='https://xxxxxxxxxx.azuredatabricks.net'

[System.Environment]::SetEnvironmentVariable("SPETLR_DATABRICKS_CONNECT", $true, [System.EnvironmentVariableTarget]::User)

[System.Environment]::SetEnvironmentVariable("DATABRICKS_CONFIG_PROFILE", $my_profile, [System.EnvironmentVariableTarget]::User)

databricks auth login --configure-cluster --profile $my_profile --host $host_url

databricks auth env --profile $my_profile
``` 

[Additional info on Databricks Connect can be found here...](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html)
