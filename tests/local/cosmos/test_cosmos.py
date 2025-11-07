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
    Also, that the edge-cases with wrongly or


    """

    def test_01_key_auth_sets_attributes(self):
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
        with self.assertRaisesRegex(ValueError, "account_name or endpoint must be set"):
            CosmosDb(account_key="KEY", database="db")

    def test_04_raises_when_aad_missing_fields(self):
        mod = _get_module(CosmosDb)

        class DummyClient:
            def __init__(self, endpoint, credential):
                pass

        with patch.object(mod, "CosmosClient", DummyClient):
            with self.assertRaisesRegex(ValueError, "AAD auth selected"):
                CosmosDb(
                    account_key=None,
                    database="db",
                    endpoint="https://acc.documents.azure.com:443/",
                    tenant_id="tid",
                )

    def test_05_multiple_auth_types_used(self):
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
