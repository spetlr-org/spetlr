import unittest

from atc.sql import SqlServer


class DeliverySqlServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_from_connection_string(self):
        server = "cltest.database.windows.net"
        port = "1433"
        database = "testdatabase!@_"
        user = "testuser"
        password = "testpwd"

        connection_string = f"""
        Server=tcp:{server},{port};
        Database={database};
        User ID={user};
        Password={password};
        Encrypt=true;
        Connection Timeout=30;"""

        test_server = SqlServer(connection_string=connection_string)
        sql_server = SqlServer(server, database, user, password, port)

        self.assertEqual(test_server.odbc, sql_server.odbc)
        self.assertEqual(test_server.url, sql_server.url)
        self.assertEqual(test_server.properties, sql_server.properties)

    def test_from_connection_string2(self):
        server = "cltest.database.windows.net"
        port = "1433"
        database = "test.database!@"
        user = "test_user1"
        password = "_@b3&yy"

        connection_string = f"""
        Addr={server};
        Initial Catalog={database};
        UID={user};
        PWD={password};
        Encrypt=true;
        Connection Timeout=30;"""

        test_server = SqlServer(connection_string=connection_string)
        sql_server = SqlServer(server, database, user, password, port)

        self.assertEqual(test_server.odbc, sql_server.odbc)
        self.assertEqual(test_server.url, sql_server.url)
        self.assertEqual(test_server.properties, sql_server.properties)

    def test_from_connection_string_raises(self):
        connection_string = "Not data=tcp:, not pass, not user"

        self.assertRaises(
            ValueError, SqlServer, None, None, None, None, None, connection_string
        )

    def test_not_enough_params_raises(self):
        self.assertRaises(
            ValueError,
            SqlServer,
            "test",
            None,
            "something missing",
            "nothere",
            "404",
            None,
        )
