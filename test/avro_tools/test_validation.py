import io
import unittest
from contextlib import redirect_stdout

import pytest

from avro_tools.validation import AvroValidator, AvroValidationError
from test import *


class TestAvroValidation(unittest.TestCase):

    @staticmethod
    def read_avro_file(file_name: str) -> str:
        file_path = os.path.join(RESOURCES, 'avro', file_name)
        with open(file_path, mode='r') as fh:
            txt = fh.read()
        return txt

    def test_validate_avro_schema_definition_valid(self):
        """Tests valid AVRO schema definition"""
        avro = self.read_avro_file("avro_schema_valid.avsc")
        success, exception = AvroValidator.validate_schema_definition(avro)
        self.assertTrue(success)
        self.assertIsNone(exception)

    def test_validate_avro_schema_definition_invalid_avro(self):
        """Tests invalid AVRO format"""
        avro = self.read_avro_file("avro_schema_invalid_avro.avsc")
        success, exception = AvroValidator.validate_schema_definition(avro)
        self.assertFalse(success)
        self.assertIsInstance(exception, AvroValidationError)
        self.assertIn(AvroValidationError.AVRO_ERROR, exception.msg)

    def test_validate_avro_schema_definition_invalid_json(self):
        """Tests invalid JSON format"""
        avro = self.read_avro_file("avro_schema_invalid_json.avsc")
        success, exception = AvroValidator.validate_schema_definition(avro)
        self.assertFalse(success)
        self.assertIsInstance(exception, AvroValidationError)
        self.assertIn(AvroValidationError.JSON_ERROR, exception.msg)

    def test_validate_avro_file(self):
        """Tests validation of file containing valid AVRO schema definition"""
        file_path = os.path.join(RESOURCES, 'avro', "avro_schema_valid.avsc")
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            result = AvroValidator.validate_avro_schema_file(file_path=file_path)
        self.assertTrue(result)
        msg = stdout.getvalue()
        self.assertIn('SUCCESS', msg)

    def test_validate_all_in_folder(self):
        """Tests validation of all files in a folder, where one file is valid and two files are invalid"""
        folder = os.path.join(RESOURCES, 'avro')
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            valid_files = AvroValidator.validate_all_in_folder(folder)
        self.assertEqual(1, valid_files)
        msg = stdout.getvalue()
        self.assertEqual(2, msg.count('FAIL'))
        self.assertEqual(1, msg.count('SUCCESS'))


if __name__ == '__main__':
    pytest.main()
