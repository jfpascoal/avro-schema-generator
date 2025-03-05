import unittest
import os
import shutil
import pytest

from configuration import Configuration, InvalidConfigurationError


class TestConfiguration(unittest.TestCase):
    TEST_DIR = os.path.join(os.getcwd(), 'tmp_test_configuration')
    TEST_CONFIG = os.path.join(TEST_DIR, '.config')
    Configuration._CONFIG_PATH = TEST_CONFIG

    @classmethod
    def setUpClass(cls):
        os.mkdir(cls.TEST_DIR)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.TEST_DIR)

    @staticmethod
    def write_to_config_file(config: str, file_path: str = TEST_CONFIG) -> None:
        with open(file_path, 'w') as fh:
            fh.write(config)

    def test_configuration_attributes(self):
        self.write_to_config_file("""
        [DEFAULT]
        connector = aaa
        connector_mapper = bbb 
        connector_csv_path = ccc
        connector_tables = ddd
        db_system = eee
        db_server = fff
        db_name = ggg
        db_schema = hhh
        avro_namespace = iii
        avro_output_path = path/jjj
        avro_all_nullable = false
        """)

        config = Configuration()
        expected = {
            "connector": "aaa",
            "connector_mapper": "bbb",
            "connector_csv_path": "ccc",
            "connector_tables": "ddd",
            "db_system": "eee",
            "db_server": "fff",
            "db_name": "ggg",
            "db_schema": "hhh",
            "avro_namespace": "iii",
            "avro_output_path": "path/jjj",
            "avro_all_nullable": False
        }
        actual = {k: v for k, v in config.__dict__.items() if not k.startswith('_')}
        self.assertEqual(expected, actual)

    def test_invalid_configuration(self):
        self.write_to_config_file("""
        [DEFAULT]
        connector_csv_path = ccc
        connector_tables = ddd
        db_system = eee
        db_server = fff
        db_name = ggg
        db_schema = hhh
        avro_namespace = iii
        avro_output_path = path/jjj
        avro_all_nullable = false
        """)

        with self.assertRaises(InvalidConfigurationError) as cm:
            Configuration()
        exception_msg = cm.exception.msg
        self.assertIn("connector, connector_mapper", exception_msg)

    def test_configuration_defaults(self):
        self.write_to_config_file("""
        [DEFAULT]
        connector = connector
        connector_mapper = mapper
        """)

        config = Configuration()
        expected = {
            "connector": "connector",
            "connector_mapper": "mapper",
            "connector_csv_path": None,
            "connector_tables": None,
            "db_system": None,
            "db_server": None,
            "db_name": None,
            "db_schema": None,
            "avro_namespace": None,
            "avro_output_path": os.getcwd(),
            "avro_all_nullable": False
        }
        actual = {k: v for k, v in config.__dict__.items() if not k.startswith('_')}

        self.assertEqual(expected, actual)



class TestParseConfig(TestConfiguration):

    def test_parse_empty_config(self):
        config = "[DEFAULT]"
        self.write_to_config_file(config)

        result = Configuration.parse_config(self.TEST_CONFIG)
        self.assertEqual(result, {})

    def test_parse_config(self):
        config = """
        [DEFAULT]
        key1 = value1
        key2 =
        key3
        key4 = 0
        """
        self.write_to_config_file(config)
        result = Configuration.parse_config(self.TEST_CONFIG)
        expected_result = {
            "key1": "value1",
            "key2": None,
            "key3": None,
            "key4": "0"
        }
        self.assertEqual(expected_result, result)



if __name__ == '__main__':
    pytest.main()
