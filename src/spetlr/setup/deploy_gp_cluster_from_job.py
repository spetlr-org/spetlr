import sys
from typing import Dict

from databricks.sdk.service.compute import ClusterDetails

from spetlr.db_auto import getDbApi
from spetlr.entry_points.generalized_task_entry_point import (
    prepare_keyword_arguments,
    prepare_kwargs_from_argv,
)
from spetlr.reporting.JobReflection import JobReflection
from spetlr.setup.deploy_gp_cluster import deploy_gp_cluster


def deploy_gp_cluster_from_job(
    *, cluster_name: str, extra_cluster_details: Dict[str, str]
):
    """Deploy a general purpose cluster og a given name
    where the entire cluster specification and libraries
    are obtained by reflection on the calling job."""
    db = getDbApi()
    job_details = JobReflection.get_job_api_details()
    base_cluster_spec = job_details.cluster_spec.new_cluster
    libraries = job_details.cluster_spec.libraries
    if not base_cluster_spec or not libraries:
        raise Exception("Expected to be called with new cluster specification.")

    # now prepare the cluster details to use in creation and edit operations
    cluster_details_dict = base_cluster_spec.as_dict()
    cluster_details_dict.update(extra_cluster_details)
    # only keep those parts of the job cluster specification that are useful
    # for the creation of the gp cluster
    cluster_details_dict = prepare_keyword_arguments(
        db.clusters.create, cluster_details_dict
    )
    cluster_details_dict["cluster_name"] = cluster_name
    cluster_details = ClusterDetails.from_dict(cluster_details_dict)
    deploy_gp_cluster(db=db, cluster_spec=cluster_details, libraries=libraries)


def main():
    """Entry point for when this is called from a job"""
    extra_cluster_details = prepare_kwargs_from_argv(sys.argv)
    cluster_name = extra_cluster_details.pop("cluster_name")
    deploy_gp_cluster_from_job(
        cluster_name=cluster_name, extra_cluster_details=extra_cluster_details
    )
