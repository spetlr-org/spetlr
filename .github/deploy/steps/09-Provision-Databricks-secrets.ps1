
# The databricks scope is named after the keyvaultname
$scope = "test"

New-DatabricksScope -name $scope

$value =
databricks secrets put --scope $scope --key "SqlServer--DatabricksUser" --string-value $value


$value =
databricks secrets put --scope $scope --key "SqlServer--DatabricksUserPassWord" --string-value $value



