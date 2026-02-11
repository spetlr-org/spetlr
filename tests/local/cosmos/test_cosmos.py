# tests/test_cosmosdb_auth_unittest.py
import importlib
import unittest
from unittest.mock import patch

from spetlr.cosmos import CosmosDb


def _get_module(obj):
    return importlib.import_module(obj.__module__)


class TestCosmosLocal(unittest.TestCase):
    """
    The intention of the test, is to ensure that the configs are setted correctly.
    Also, that the edge-cases with wrongly or correct
    configurations propogates as we expect
    """

    def test_01_key_auth_sets_attributes(self):
        """
        Verifies that providing an account key selects key-auth mode,
        builds a data-plane CosmosClient with the key,
        and sets Spark config (accountEndpoint, database, accountKey)
        while leaving AAD settings unset.
        """
        mod = _get_module(CosmosDb)

        calls = {}

        class DummyClient:
            def __init__(self, endpoint, credential):
                calls["endpoint"] = endpoint
                calls["credential"] = credential

        with patch.object(mod, "CosmosClient", DummyClient):
            db = CosmosDb(
                account_key="KEY123",
                database="UserAuthentication",
                account_name="mycosmosacct",
            )

        self.assertEqual(db._auth_mode, "key")
        self.assertEqual(db.account_key, "KEY123")
        self.assertEqual(db.endpoint, "https://mycosmosacct.documents.azure.com:443/")
        self.assertEqual(db.config["spark.cosmos.accountEndpoint"], db.endpoint)
        self.assertEqual(db.config["spark.cosmos.database"], "UserAuthentication")
        self.assertEqual(db.config["spark.cosmos.accountKey"], "KEY123")
        self.assertNotIn("spark.cosmos.auth.type", db.config)
        self.assertEqual(calls["endpoint"], db.endpoint)
        self.assertEqual(calls["credential"], "KEY123")

    def test_02_aad_auth_sets_attributes(self):
        """
        Verifies that supplying full AAD credentials selects aad-auth mode,
        builds a data-plane CosmosClient with ClientSecretCredential,
        and sets Spark config for ServicePrincipal (tenantId, clientId, clientSecret)
        without setting accountKey.
        """
        mod = _get_module(CosmosDb)

        class DummyCred:
            def __init__(self, tenant_id, client_id, client_secret):
                self.tenant_id = tenant_id
                self.client_id = client_id
                self.client_secret = client_secret

        class DummyClient:
            def __init__(self, endpoint, credential):
                self.endpoint = endpoint
                self.credential = credential

        with patch.object(mod, "ClientSecretCredential", DummyCred), patch.object(
            mod, "CosmosClient", DummyClient
        ):
            db = CosmosDb(
                account_key=None,
                database="UserAuthentication",
                endpoint="https://acc.documents.azure.com:443/",
                tenant_id="tid",
                client_id="cid",
                client_secret="sec",
                subscription_id="sid",
                resource_group="rg",
                account_name="accname",
            )

        self.assertEqual(db._auth_mode, "aad")
        cfg = db.config
        self.assertEqual(cfg["spark.cosmos.auth.type"], "ServicePrincipal")
        self.assertEqual(cfg["spark.cosmos.account.tenantId"], "tid")
        self.assertEqual(cfg["spark.cosmos.auth.aad.clientId"], "cid")
        self.assertEqual(cfg["spark.cosmos.auth.aad.clientSecret"], "sec")
        self.assertNotIn("spark.cosmos.accountKey", cfg)
        self.assertIsInstance(db.client, DummyClient)
        self.assertIsInstance(db.client.credential, DummyCred)

    def test_03_raises_when_no_account_name_or_endpoint(self):
        """
        Ensures the constructor raises ValueError when neither account_name
        nor endpoint is provided,
        since one is required to form the account endpoint.
        """
        with self.assertRaisesRegex(ValueError, "account_name or endpoint must be set"):
            CosmosDb(account_key="KEY", database="db")

    def test_04_raises_when_aad_missing_fields(self):
        """
        Ensures the constructor raises ValueError ('Missing credentials')
        when AAD is intended but required parameters are incomplete
        (e.g., only tenant_id given).
        """
        mod = _get_module(CosmosDb)

        class DummyClient:
            def __init__(self, endpoint, credential):
                pass

        with patch.object(mod, "CosmosClient", DummyClient):
            with self.assertRaisesRegex(ValueError, "Missing credentials"):
                CosmosDb(
                    account_key=None,
                    database="db",
                    endpoint="https://acc.documents.azure.com:443/",
                    tenant_id="tid",
                )

    def test_05_multiple_auth_types_used(self):
        """
        Ensures the constructor rejects mixed authentication by
        raising ValueError when both an account_key and any AAD credentials
        are provided at the same time.
        """
        mod = _get_module(CosmosDb)
        calls = {}

        class DummyClient:
            def __init__(self, endpoint, credential):
                calls["endpoint"] = endpoint
                calls["credential"] = credential

        with patch.object(mod, "CosmosClient", DummyClient):
            with self.assertRaisesRegex(
                ValueError, "Both account_key and client credentials"
            ):
                CosmosDb(
                    account_key="KEY123",
                    database="UserAuthentication",
                    account_name="mycosmosacct",
                    tenant_id="tid",
                    client_id="cid",
                    client_secret="sec",
                )

    def test_06_mgmt_raises_on_key_mode(self):
        """
        Confirms that calling _mgmt() in key-auth mode
        is forbidden and raises RuntimeError,
        since the ARM management client is only available with AAD auth.
        """
        mod = _get_module(CosmosDb)

        class DummyClient:
            def __init__(self, endpoint, credential):
                pass

        # Key mode
        with patch.object(mod, "CosmosClient", DummyClient):
            db = CosmosDb(
                account_key="KEY123",
                database="UserAuthentication",
                account_name="mycosmosacct",
            )

        with self.assertRaisesRegex(RuntimeError, "ARM management client"):
            db._mgmt()

    def test_07_mgmt_constructs_in_aad_mode(self):
        """
        Confirms that calling _mgmt() in aad-auth mode returns a management (ARM)
        client constructed with the provided TokenCredential and subscription ID.
        """
        mod = _get_module(CosmosDb)

        class DummyCred:
            def __init__(self, tenant_id, client_id, client_secret):
                self.tenant_id = tenant_id
                self.client_id = client_id
                self.client_secret = client_secret

        class DummyMgmt:
            def __init__(self, credential, subscription_id):
                self.credential = credential
                self.subscription_id = subscription_id

        class DummyDataPlaneClient:
            def __init__(self, endpoint, credential):
                pass

        with patch.object(mod, "ClientSecretCredential", DummyCred), patch.object(
            mod, "CosmosDBManagementClient", DummyMgmt
        ), patch.object(mod, "CosmosClient", DummyDataPlaneClient):
            db = CosmosDb(
                account_key=None,
                database="UserAuthentication",
                endpoint="https://acc.documents.azure.com:443/",
                tenant_id="tid",
                client_id="cid",
                client_secret="sec",
                subscription_id="sid",
                resource_group="rg",
                account_name="accname",
            )

            mgmt = db._mgmt()
            self.assertIsInstance(mgmt, DummyMgmt)
            self.assertEqual(mgmt.subscription_id, "sid")
            self.assertIsInstance(mgmt.credential, DummyCred)
            self.assertEqual(mgmt.credential.tenant_id, "tid")
            self.assertEqual(mgmt.credential.client_id, "cid")
            self.assertEqual(mgmt.credential.client_secret, "sec")
