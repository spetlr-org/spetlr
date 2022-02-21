
function Get-DatabricksInstancePool {

    param (
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $name
    )

    $pools = ((Convert-Safe-FromJson -text (databricks instance-pools list --output JSON)).instance_pools) | Where-Object { $_.instance_pool_name -eq $name }

    if ($pools.Count -eq 0) {
      throw "$name does not exist"
    }

    return $pools[0].instance_pool_id
  }
