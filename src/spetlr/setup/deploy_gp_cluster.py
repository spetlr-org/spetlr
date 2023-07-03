import sys
from dataclasses import fields
from typing import List, Dict

from spetlr.db_auto import getDbApi
from spetlr.entry_points.generalized_task_entry_point import prepare_kwargs_from_argv, prepare_keyword_arguments
from spetlr.reporting.JobReflection import JobReflection
from databricks.sdk.service.compute import ClusterInfo, Library, State, BaseClusterInfo
from databricks.sdk import WorkspaceClient

def main():
    """Entry point for when this is called from a job"""
    extra_cluster_details = prepare_kwargs_from_argv(sys.argv)
    cluster_name = extra_cluster_details.pop('cluster_name')
    deploy_gp_cluster_from_job_cluster(cluster_name, extra_cluster_details)


def deploy_gp_cluster_from_job_cluster(cluster_name:str, extra_cluster_details:Dict[str,str]):
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
    cluster_details_dict = prepare_keyword_arguments(db.clusters.create,cluster_details_dict)
    cluster_details_dict['cluster_name'] = cluster_name
    cluster_details = BaseClusterInfo.from_dict(cluster_details_dict)
    deploy_gp_cluster(db, cluster_details, libraries)

def deploy_gp_cluster(db:WorkspaceClient,cluster_spec:BaseClusterInfo,libraries:List[Library]):
    """Deploy a gp cluster with the given specification and libraries,
    can be called from anywhere as long as the WorkspaceClient works."""

    libraries = set(libraries)
    # the following dict contains complex objects like AzureAttributes
    cluster_details_args = dict((field.name, getattr(cluster_spec, field.name)) for field in fields(cluster_spec))


    clusters = {cl.cluster_name:cl for cl in db.clusters.list()}
    if cluster_spec.cluster_name not in clusters:
        db.clusters.create(**cluster_details_args)
        clusters = {cl.cluster_name: cl for cl in db.clusters.list()}

    existing:ClusterInfo = clusters[cluster_spec.cluster_name]

    db.clusters.edit(cluster_id=existing.cluster_id,
                              **cluster_details_args)

    existing_libs = set(lib.library for lib in db.libraries.cluster_status(cluster_id=existing.cluster_id).library_statuses)
    if set(libraries) == existing_libs:
        return

    # we know that there are differences. To change libraries the cluster must run:
    db.clusters.ensure_cluster_is_running(cluster_id=existing.cluster_id)

    # Any to uninstall?
    if existing_libs - set(libraries):
        db.libraries.uninstall(cluster_id=existing.cluster_id, libraries=list(existing_libs - set(libraries)))
        any_uninstall = True
    else:
        any_uninstall = False

    # Any to install?
    if set(libraries) - existing_libs:
        db.libraries.install(cluster_id=existing.cluster_id, libraries=list(set(libraries) - existing_libs))

    if any_uninstall:
        db.clusters.restart(cluster_id=existing.cluster_id)



