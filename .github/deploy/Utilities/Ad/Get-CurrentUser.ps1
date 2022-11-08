function Get-CurrentUser {


  $currentUser = az ad signed-in-user show

  Throw-WhenError -output $output

  return Convert-Safe-FromJson -text $currentUser

}
