
# The databricks scope is named after the keyvaultname
$scope = "test"

New-DatabricksScope -name $scope

# Insert SQL credentials to secrets
# Todo: insert into key-dict instead
databricks secrets put --scope $scope --key "SqlServer--DatabricksUser" --string-value $dbUserName
databricks secrets put --scope $scope --key "SqlServer--DatabricksUserPassword" --string-value $dbUserPassword



