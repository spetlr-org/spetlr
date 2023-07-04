$jsonOutput = databricks clusters list --output JSON
$clusters = ConvertFrom-Json $jsonOutput

$desiredClusterNames = @("jobdeploy", "localdeploy")

# Filter clusters with desired names
$filteredClusters = $clusters | Where-Object { $desiredClusterNames -contains $_.cluster_name }

if ($filteredClusters.Count -eq 2) {
    # Two clusters with desired names found
    Write-Host "PASSED: Found exactly two clusters with the desired cluster names."

} else {
    # Not exactly two clusters with desired names found
    $errorMessage = "FAILED: Did not find exactly two clusters with the desired cluster names."
    throw $errorMessage
}
