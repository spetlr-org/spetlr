param (
  # in the pipeline we wish to test with multiple versions if we need.
  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]
  $cluster_env,

  [Parameter(Mandatory = $true)]
  [ValidateNotNullOrEmpty()]
  [string]
  $sparkLibs
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

