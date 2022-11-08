Write-Host "  Install powershell dependencies..." -ForegroundColor Yellow
#
# Install-Module -Name Az.Accounts -Scope CurrentUser -Repository PSGallery -Force -AllowClobber
Write-Host "  Install SqlServer (Invoke-SqlCmd)" -ForegroundColor DarkYellow
Install-Module -Name SqlServer -Force

