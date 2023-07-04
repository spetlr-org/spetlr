import unittest

from spetlr.setup.deploy_gp_cluster_from_job import deploy_gp_cluster_from_job
from spetlr.spark import Spark


class TestClusterCreation(unittest.TestCase):
    @unittest.skipUnless(
        Spark.version() == Spark.DATABRICKS_RUNTIME_11_3,
        "Only deploy this GP cluster once",
    )
    def test_01_create_gp_cluster(self):
        deploy_gp_cluster_from_job(
            cluster_name="jobdeploy",
            extra_cluster_details={"autotermination_minutes": "10"},
        )
