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

    def test_spn_url(self):
        server = "cltest.database.windows.net"
        database = "testdatabase!@_"
        spnid = "spnuser"
        spnpassword = "spnpwd"
        timeout = 180

        sql_server = SqlServer(server, database, spnid=spnid, spnpassword=spnpassword)

        expected_jdbc_url = (
            f"jdbc:sqlserver://{server}:1433;"
            f"database={database};"
            f"queryTimeout=0;"
            f"loginTimeout={timeout};"
            "driver=com.microsoft.sqlserver.jdbc.SQLServerDriver;"
            f"AADSecurePrincipalId={spnid};"
            f"AADSecurePrincipalSecret={spnpassword};"
            f"encrypt=true;"
            f"trustServerCertificate=false;"
            f"hostNameInCertificate=*.database.windows.net;"
            f"authentication=ActiveDirectoryServicePrincipal"
        )

        expected_odbc_url = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"PORT=1433;"
            f"UID={spnid};"
            f"PWD={spnpassword};"
            f"Connection Timeout={timeout}"
            f";Authentication=ActiveDirectoryServicePrincipal"
        )

        self.assertEqual(sql_server.url, expected_jdbc_url)
        self.assertEqual(sql_server.odbc, expected_odbc_url)

    def test_sqluseradmin_url(self):
        server = "cltest.database.windows.net"
        database = "testdatabase!@_"
        user = "user"
        password = "pass"
        timeout = 180

        sql_server = SqlServer(server, database, username=user, password=password)

        expected_jdbc_url = (
            f"jdbc:sqlserver://{server}:1433;"
            f"database={database};"
            f"queryTimeout=0;"
            f"loginTimeout={timeout};"
            "driver=com.microsoft.sqlserver.jdbc.SQLServerDriver;"
            f"user={user};password={password}"
        )

        expected_odbc_url = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"PORT=1433;"
            f"UID={user};"
            f"PWD={password};"
            f"Connection Timeout={timeout}"
        )

        self.assertEqual(sql_server.url, expected_jdbc_url)
        self.assertEqual(sql_server.odbc, expected_odbc_url)

    def test_both_spn_and_sqluser(self):
        server = "cltest.database.windows.net"
        database = "testdatabase!@_"
        user = "user"
        password = "password"
        spnid = "spnuser"
        spnpassword = "spnpwd"
        exptected_fail = "Use either SPN or SQL user - never both"

        with self.assertRaises(ValueError) as ctx:
            SqlServer(
                server,
                database,
                username=user,
                password=password,
                spnid=spnid,
                spnpassword=spnpassword,
            )
        self.assertEqual(exptected_fail, str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            SqlServer(
                server,
                database,
                username=user,
                spnpassword=spnpassword,
            )
        self.assertEqual(exptected_fail, str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            SqlServer(
                server,
                database,
                password=password,
                spnid=spnid,
            )
        self.assertEqual(exptected_fail, str(ctx.exception))
