import argparse
import json
from dataclasses import fields
from typing import List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import BaseClusterInfo, Library

from spetlr.db_auto import getDbApi
from spetlr.entry_points.generalized_task_entry_point import prepare_keyword_arguments


def main():
    parser = argparse.ArgumentParser(description="Databricks Cluster Configuration")
    parser.add_argument(
        "--cluster_json", type=str, help="JSON string specifying the Databricks cluster"
    )
    parser.add_argument(
        "--libraries_json",
        type=str,
        help="JSON string specifying the cluster libraries",
    )

    args = parser.parse_args()

    deploy_gp_cluster(
        cluster_spec=BaseClusterInfo.from_dict(json.loads(args.cluster_json)),
        libraries=[Library.from_dict(lib) for lib in json.loads(args.libraries_json)],
        db=getDbApi(),
    )


def deploy_gp_cluster(
    *,
    cluster_spec: BaseClusterInfo,
    libraries: List[Library],
    db: WorkspaceClient = None,
):
    """Deploy a gp cluster with the given specification and libraries,
    can be called from anywhere as long as the WorkspaceClient works."""
    db = db or getDbApi()
    # the following dict contains complex objects like AzureAttributes
    cluster_details_args = dict(
        (field.name, getattr(cluster_spec, field.name))
        for field in fields(cluster_spec)
    )
    cluster_details_args = prepare_keyword_arguments(
        db.clusters.create_and_wait, cluster_details_args, warn=False
    )

    # Check if a cluster of this name exists
    clusters = [
        cl for cl in db.clusters.list() if cl.cluster_name == cluster_spec.cluster_name
    ]
    if clusters:
        existing = clusters[0]
        # if it exists we must edit it to ensure consistency
        print(
            f"Found existing cluster with name {cluster_spec.cluster_name}. "
            "Now updating it..."
        )
        db.clusters.edit_and_wait(
            cluster_id=existing.cluster_id, **cluster_details_args
        )
        # I cannot at the time of this development say for sure which properties
    else:
        print(
            f"Found no cluster with name {cluster_spec.cluster_name}. "
            "Now creating it..."
        )
        existing = db.clusters.create_and_wait(**cluster_details_args)

    existing_libs = list(
        lib.library
        for lib in (
            db.libraries.cluster_status(cluster_id=existing.cluster_id).library_statuses
            or []
        )
    )

    # We need to turn the library list
    # into a hashable set to simplify taking the differences.
    existing_libs_dict = {json.dumps(lib.as_dict()): lib for lib in existing_libs}
    libraries_dict = {json.dumps(lib.as_dict()): lib for lib in libraries}
    libs_key_set = set(libraries_dict.keys())
    existing_key_set = set(existing_libs_dict.keys())

    # are we already in agreement?
    if libs_key_set == existing_key_set:
        print("Libraries are already as specified. All done.")
        return

    print("We need to update libraries. Ensure cluster is running...")
    # we know that there are differences. To change libraries the cluster must run:
    db.clusters.ensure_cluster_is_running(cluster_id=existing.cluster_id)

    # Any to uninstall?
    if existing_key_set - libs_key_set:
        print("Now uninstalling libraries...")
        db.libraries.uninstall(
            cluster_id=existing.cluster_id,
            libraries=list(
                existing_libs_dict[key] for key in (existing_key_set - libs_key_set)
            ),
        )
        any_uninstall = True
    else:
        any_uninstall = False

    # Any to install?
    if libs_key_set - existing_key_set:
        print("Now installing libraries...")
        db.libraries.install(
            cluster_id=existing.cluster_id,
            libraries=list(
                libraries_dict[key] for key in (libs_key_set - existing_key_set)
            ),
        )

    if any_uninstall:
        print("We uninstalled some libraries. Restart to ensure correct functioning...")
        db.clusters.restart(cluster_id=existing.cluster_id)
