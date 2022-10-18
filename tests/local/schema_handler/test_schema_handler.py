import unittest
from textwrap import dedent

from atc.config_master import SchemaHandler
from tests.local.schema_handler.schemas import schemas


class TestGetSchema(unittest.TestCase):
    def test_01_schema1(self):

        schema_handler = SchemaHandler(schema_modules=schemas)

        expected_str = dedent(
            """\
            a int,
            b int COMMENT "really? is that it?",
            c string,
            cplx struct<someId:string,details:struct<id:string>,blabla:array<int>>,
            d timestamp,
            m map<int,string>,
            p decimal(10,3),
            final string"""
        )

        schema_str = schema_handler.get_schema("test_schema")

        self.assertEqual(expected_str, schema_str)


if __name__ == "__main__":
    unittest.main()
