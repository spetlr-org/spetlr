param (

  [Parameter(Mandatory=$false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $testJobDetails= "test_job_details.json"

)

if(-not (Test-Path -Path $testJobDetails -PathType Leaf)){
    Write-Host -ForegroundColor Red "ERROR: The file $testJobDetails does not exist. Please run submit_test_job.ps1 first."
    EXIT 1
}

# This script submits a run to databricks to execute the complete test-suite

# import utility functions
. "$PSScriptRoot\..\deploy\Utilities\all.ps1"

$job_details = Get-Content $testJobDetails | ConvertFrom-Json

$runId = $job_details.runId
$testDir = $job_details.testDir
$resultLogs = $job_details.logOut
$srcDir = "$PSScriptRoot/"


# report on status
Write-Host "============================================================================"
Write-Host "Run with ID $runId"
Write-Host "Test dir $testDir"

$run = (databricks runs get --run-id $runId | ConvertFrom-Json)
Write-Host "Run url: $($run.run_page_url)"
$clusterID = $run.cluster_instance.cluster_id
$state = ""
$state_msg = ""
while ($run.end_time -eq 0){
    Start-Sleep -Seconds 5

    $run = (databricks runs get --run-id $runId | ConvertFrom-Json)
    $clusterID = $run.cluster_instance.cluster_id

    # display the messages if they have changed
    if($run.state.life_cycle_state -ne $state){
        $state = $run.state.life_cycle_state
        Write-Host "Run is now in state $state"
    }
    if($run.state.state_message -ne $state_msg){
        $state_msg = $run.state.state_message
        if($state_msg){
            Write-Host "Run message: $state_msg"
        }
    }
}

Write-Host "Run has ended. Now fetching logs..."
# the job is complete. Get the logs
$timeout = 60
$localLogs="$srcDir/test_job_results_$($job_details.submissionTime).log"
do{
    dbfs cp --overwrite $resultLogs $localLogs *>$null
    if($LASTEXITCODE -eq 0) {break;}

    $timeout-=1
    Start-Sleep -Seconds 1
} until( $timeout -lt 1 )

if($timeout -lt 1){
    throw "Unable to get logs from $resultLogs"
}

Write-Host "Logs can be seen in $localLogs"
Write-Host "============================================================================"

Get-Content $localLogs

Write-Host "Overall the result is $($run.state.result_state)"
if($run.state.result_state -eq "SUCCESS"){
    EXIT 0
}else {
    EXIT 1
}
