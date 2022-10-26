
$srcDir = "$PSScriptRoot/../../.."

Push-Location -Path $srcDir

pip install dbx

dbx configure
copy "$srcDir/.github/submit/sparklibs.json" "$srcDir/tests/cluster/mount/"

$mountsJson = @(
  @{
    storageAccountName=$resourceName
    secretScope="secrets"
    clientIdName="Databricks--ClientId"
    clientSecretName="Databricks--ClientSecret"
    tenantIdName="Databricks--TenantId"
    containers = [array]$($dataLakeContainers | ForEach-Object{ $_.name })
  }
)

$mountsJson | ConvertTo-Json -Depth 4 | Set-Content "$srcDir/tests/cluster/mount/mounts.json"

dbx deploy --deployment-file  "$srcDir/tests/cluster/mount/setup_job.yml.j2"

dbx launch --job="Setup Mounts" --trace --kill-on-sigterm

Pop-Location
