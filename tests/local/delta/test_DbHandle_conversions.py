import unittest

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_sql_code_to_config
from spetlr.delta import DbHandle


class TestTableSpecConversions(unittest.TestCase):
    def test_DbHandle_from_sql(self):
        config = parse_sql_code_to_config(
            """
            -- SPETLR.CONFIGURATOR key: MyDbAlias
            CREATE DATABASE IF NOT EXISTS my_db_name
            COMMENT "Really great db"
            LOCATION "/somewhere/over/the/rainbow";
            """
        )

        k, v = config.popitem()
        self.assertEqual(k, "MyDbAlias")
        c = Configurator()
        c.register(k, v)
        db = DbHandle.from_tc(k)
        self.assertEqual(
            db,
            DbHandle(
                name="my_db_name",
                location="/somewhere/over/the/rainbow",
                comment="Really great db",
            ),
        )

    def test_DbHandle_to_sql(self):
        db = DbHandle(
            name="my_db_name",
            location="/somewhere/over/the/rainbow",
            comment="Really great db",
        )
        self.assertEqual(
            db.get_create_sql(),
            (
                "CREATE DATABASE IF NOT EXISTS my_db_name\n"
                '  COMMENT="Really great db"\n'
                '  LOCATION "/somewhere/over/the/rainbow"'
            ),
        )

    def test_DbHandle_repr(self):
        db = DbHandle(
            name="my_db_name",
            location="/somewhere/over/the/rainbow",
            comment="Really great db",
        )
        as_text = repr(db)

        self.assertEqual(db, eval(as_text))
