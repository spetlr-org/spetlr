param (
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]
    $uniqueRunId
)

# $uniqueRunId = ""

$resourceGroupName  = "githubspetlr${$uniqueRunId}"  # This name is also used in the Terraform section
$resourceName = "githubspetlr${$uniqueRunId}"  # This name is also used in the Terraform section

Write-Host "Resource group name is $resourceGroupName"
Write-Host "Resource name is $resourceName"

$kvDbId = "Captain--ClientId"  # This secret is also used in the Terraform section
$kvDbSecret = "Captain--DbSecret"  # This secret is also used in the Terraform section