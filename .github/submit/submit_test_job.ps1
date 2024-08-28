param (
  # # to submit parallel runs, you must specify this parameter
  # [Parameter(Mandatory = $false)]
  # [ValidateNotNullOrEmpty()]
  # [string]
  # $testJobDetails = "test_job_details.json",

  # in the pipeline we wish to test with multiple versions.
  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $cluster_env = "cluster_env_91.json",

  [Parameter(Mandatory = $false)]
  [ValidateNotNullOrEmpty()]
  [string]
  $sparkLibs = "sparklibs91.json"
)

$repoRoot = (git rev-parse --show-toplevel)

Push-Location -Path $repoRoot

# submit test
Write-Host "Now Submitting"

spetlr-test-job submit `
  --tests tests/ `
  --tasks-from tests/cluster/ `
  --cluster-file $repoRoot/.github/submit/$cluster_env `
  --requirements-file requirements_test.txt `
  --sparklibs-file $repoRoot/.github/submit/$sparklibs `
  --out-json test.json `
  --upload-to "workspace"

# Step 3: wait for test
Write-Host "Now Waiting for test"

spetlr-test-job fetch --runid-json test.json

Pop-Location

