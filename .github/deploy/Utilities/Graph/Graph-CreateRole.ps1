# Graph equivalent of az role assignment create
function Graph-CreateRole {

    param (
      [Parameter(Mandatory=$true)]
      [string]
      $principalId,
      [Parameter(Mandatory=$true)]
      [string]
      $roleDefinitionId
    )
    $helpVar = "@odata.type"

    return Graph-Rest -method "post" -url "roleManagement/directory/roleAssignments" -body @{$helpVar= "#microsoft.graph.unifiedRoleAssignment"; principalId=$principalId;roleDefinitionId=$roleDefinitionId;DirectoryScopeId= "/"}
}
