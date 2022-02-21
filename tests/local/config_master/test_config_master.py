import unittest

from atc import ConfigMaster


class TestConfigMaster(unittest.TestCase):
    old_config: ConfigMaster

    @classmethod
    def setUpClass(cls) -> None:
        cls.old_config = ConfigMaster().reset()

        (
            ConfigMaster()
            .extras_from("tests.local.config_master.extras")
            .sql_from("tests.local.config_master.sql")
        )

    @classmethod
    def tearDownClass(cls) -> None:
        ConfigMaster().restore(cls.old_config)

    def test_config(self):
        config = ConfigMaster()
        print(config.table_details)

    def test_name(self):
        config = ConfigMaster().set_debug()
        name = config.get_name("table_id1")
        self.assertTrue(name.startswith("my_db1_"))
        self.assertTrue(name.endswith(".tbl1"))

        config.set_release()
        name = config.get_name("table_id1")
        self.assertEqual("my_db1.tbl1", name)

    def test_alias(self):
        config = ConfigMaster().set_release()
        name = config.get_name("details")
        self.assertEqual("my_db1.tbl1", name)

        config.set_debug()
        name = config.get_name("details")
        self.assertTrue(name.startswith("my_db1_"))
        self.assertTrue(name.endswith(".details"))

    def test_schema(self):
        config = ConfigMaster().set_debug()
        sql = config.get_sql("details")
        # print(sql)
        self.assertTrue(sql.find("a int, b int") > 0)

    def test_full_sql(self):
        config = ConfigMaster().set_debug()
        sqls = config.get_full_creation_sql("details")
        self.assertEqual(2, len(sqls))
        # for line in sqls:
        #     print("  ", line)

        sqls = config.get_full_creation_sql("db1")
        self.assertEqual(3, len(sqls))
        # for line in sqls:
        #     print("  ", line)


if __name__ == "__main__":
    unittest.main()
