
$srcDir = "$PSScriptRoot/../../.."

Push-Location -Path $srcDir

pip install dbx

dbx configure
copy "$srcDir/.github/submit/sparklibs.json" "$srcDir/tests/cluster/mount/"

dbx deploy --deployment-file  "$srcDir/tests/cluster/mount/setup_job.yml.j2"

dbx launch --job="Setup Mounts" --trace --kill-on-sigterm

Pop-Location
