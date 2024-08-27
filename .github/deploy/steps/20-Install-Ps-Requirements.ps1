Write-Host "  Install powershell dependencies..." -ForegroundColor Yellow

Write-Host "  Install SqlServer (Invoke-SqlCmd)" -ForegroundColor DarkYellow
Install-Module -Name SqlServer -Force

# Write-Host "  Install DBX" -ForegroundColor DarkYellow
# pip install dbx