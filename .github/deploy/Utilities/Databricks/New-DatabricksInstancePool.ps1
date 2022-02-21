
function New-DatabricksInstancePool {

    param (
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [string]
      $name,
  
      [Parameter(Mandatory = $false)]
      [ValidateNotNullOrEmpty()]
      [ValidateSet('Standard_L4s', 'Standard_L8s')]
      [string]
      $clusterNodeType = "Standard_L4s",
  
      [Parameter(Mandatory = $false)]
      [ValidateNotNullOrEmpty()]
      [int]
      $autoTerminationMinutes = 30,
  
      [Parameter(Mandatory = $false)]
      [ValidateNotNullOrEmpty()]
      [int]
      $minIdleInstances = 1,
  
      [Parameter(Mandatory = $true)]
      [ValidateNotNullOrEmpty()]
      [int]
      $maxCapacity,
  
      [Parameter(Mandatory = $false)]
      [ValidateNotNullOrEmpty()]
      [ValidateSet('9.1.x-scala2.12', '9.1.x-photon-scala2.12')]
      [string]
      $sparkVersion = "9.1.x-scala2.12"
    )
    
    $pool = @{
      instance_pool_name = $name
      node_type_id = $clusterNodeType
      idle_instance_autotermination_minutes = $autoTerminationMinutes
      min_idle_instances = $minIdleInstances
      max_capacity = $maxCapacity
      preloaded_spark_versions = @($sparkVersion)
    }
  
    Set-Content ./pool.json ($pool | ConvertTo-Json)
  
    $pools = ((Convert-Safe-FromJson -text (databricks instance-pools list --output JSON)).instance_pools) | Where-Object { $_.instance_pool_name -eq $name }
    if ($pools.Count -eq 0) {
      Write-Host "  Creating $name instance pool"
      $poolId = ((Convert-Safe-FromJson -text (databricks instance-pools create --json-file ./pool.json)).instance_pool_id)
      Write-Host "  Created new instance pool (ID=$poolId)"
    }
    else {
      $poolId = $pools[0].instance_pool_id
      Write-Host "  $name instance pool already exists (ID=$poolId)"
  
      $pool = Convert-Safe-FromJson -text (databricks instance-pools get --instance-pool-id $poolId)
      $pool.instance_pool_name = $name
      $pool.node_type_id = $clusterNodeType
      $pool.min_idle_instances = $minIdleInstances
      $pool.max_capacity = $maxCapacity
      $pool.preloaded_spark_versions = @($sparkVersion)
  
      Set-Content ./pool.json ($pool | ConvertTo-Json)
      databricks instance-pools edit --json-file ./pool.json
      Write-Host "  Updated existing instance pool (ID=$poolId)"
    }
  
    Remove-Item ./pool.json
    return $poolId
  }