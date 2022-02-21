
param (
  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $testJobDetails= "test_job_details.json",

  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $entryPoint= "atc_run_test",

  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $testCasePath= "."

)

$srcDir = "$PSScriptRoot/../.."

$libPath = "dist/atc_dataplatform-1.0.0-py3-none-any.whl"

$now = (Get-Date -Format yyyy-MM-ddTHH.mm)

# This script submits a run to databricks to execute the complete test-suite

# import utility functions
. "$PSScriptRoot\..\deploy\Utilities\all.ps1"
. "$PSScriptRoot\spark-dependencies.ps1"


# for separating tasks, we will do everything in our own dir:
$testguid = "$([guid]::NewGuid())"
$testDir = "dbfs:/test/$([guid]::NewGuid())"
dbfs mkdirs $testDir

# upload the test libraries
dbfs cp --overwrite  "$srcDir/$libPath" "$testDir/$libPath"

Push-Location -Path $srcDir

pip install pyclean
pyclean tests
dbfs cp --overwrite -r tests/ "$testDir/tests"

Pop-Location

$logOut = "$testDir/results.log"

# construct the run submission configuration
$run = @{
    run_name = "Testing Run"
    # single node cluster for now
    new_cluster= @{
        spark_version="9.1.x-scala2.12"
        spark_conf= @{
            "spark.databricks.cluster.profile"= "singleNode"
            "spark.master"= "local[*, 4]"
            "spark.databricks.delta.preview.enabled"= $true
            "spark.databricks.io.cache.enabled"= $true
        }
        azure_attributes=${
                "availability"= "ON_DEMAND_AZURE",
                "first_on_demand": 1,
                "spot_bid_max_price": -1
            }
        node_type_id= "Standard_L4s"
        custom_tags =@{
          ResourceClass="SingleNode"
        }
        spark_env_vars= @{
            PYSPARK_PYTHON= "/databricks/python3/bin/python3"
        }
        cluster_log_conf= @{
            dbfs=@{
                destination="$testDir/cluster-logs"
            }
        }
        num_workers= 0
    }
    # in addition to standard dependencies, install the lib that we just uploaded
    libraries= $spark_dependencies + @(
        @{whl = "$testDir/$libPath"}
    )
    # This scripts runs the test suite
    spark_python_task= @{
      python_file="$testDir/tests/main.py"
      parameters=@(
        "--basedir=$testDir",
        "--folder=tests/cluster"
      )
    }
  }
# we need to do this with a file because the json string is pretty funky and it breaks otherwise
Set-Content "$srcDir/run.json" ($run | ConvertTo-Json -Depth 4)
$runId = (databricks runs submit --json-file "$srcDir/run.json" | ConvertFrom-Json).run_id
Remove-Item "$srcDir/run.json"

# report on status
Write-Host "============================================================================"
Write-Host "Started Run with ID $runId"
Write-Host "Using test dir $testDir"

$run = (databricks runs get --run-id $runId | ConvertFrom-Json)
Write-Host "Run url: $($run.run_page_url)"

# Roll the test details
if(Test-Path -Path $testJobDetails -PathType Leaf){
    $old_job_details = Get-Content $testJobDetails | ConvertFrom-Json
    $new_filename = "$(Split-Path -LeafBase $testJobDetails).$($old_job_details.submissionTime).json"
    $parent = Split-Path -Parent $testJobDetails
    if ($parent){
        $new_filename = Join-Path -Path $parent -ChildPath $new_filename
    }
    Set-Content "$new_filename" ($old_job_details | ConvertTo-Json -Depth 4)
    Write-Host "Previous details at $testJobDetails were moved to $new_filename."
}

$job_details = @{
    runId=$runId
    testDir=$testDir
    submissionTime=$now
    testFolder=$testFolder
    environmentType= $environmentType
    environmentName=$environmentName
    logOut=$logOut
}
Set-Content "$testJobDetails" ($job_details | ConvertTo-Json -Depth 4)
Write-Host "test job details written to $testJobDetails"
Write-Host "you can now use fetch_test_job.ps1 to check and collect the result of your test run."
Write-Host "============================================================================"

