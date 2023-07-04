
$repoRoot = (git rev-parse --show-toplevel)

cd $repoRoot

pip install .

. "$PSScriptRoot/post_submit/deploy_gp_cluster_locally.ps1"
. "$PSScriptRoot/post_submit/deploy_gp_cluster_from_job.ps1"
