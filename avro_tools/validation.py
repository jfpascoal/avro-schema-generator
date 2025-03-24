import argparse
import json
import os

import avro.errors
import avro.schema


class AvroValidationError(Exception):
    JSON_ERROR = "Input could not be parsed as JSON"
    AVRO_ERROR = "Input could not be parsed as AVRO"

    def __init__(self, e):
        if type(e) is json.JSONDecodeError:
            self.msg = self.JSON_ERROR + f":\n{e.msg}"
        elif type(e) is avro.errors.SchemaParseException:
            self.msg = self.AVRO_ERROR + f":\n{e}"
        else:
            self.msg = f"{e}"

    def __str__(self):
        return self.msg


class AvroValidator:

    AVRO_EXT = '.avsc'

    @classmethod
    def validate_schema_definition(cls, schema: str | bytes | dict) -> (bool, AvroValidationError | None):
        """
        Checks if AVRO schema definition is syntactically valid, i.e, parsable. The schema representation can be of type
        string, bytes, or dict. If it is string or bytes, an additional validation step is carried out to ensure that it
        is parsable as a JSON.
        :param schema: schema definition as string, bytes or dict.
        :return: Tuple - first element is boolean indicating if validation was successful; if successful, the second
        element is None; otherwise, it is an AvroValidationError instance with details about the failure.
        """
        # Type enforcing
        if type(schema) not in (str, bytes, dict):
            raise TypeError(f"Input schema must be of type `str`, `dict`, or bytes. {type(schema)} provided.")

        try:
            if type(schema) in (str, bytes):
                schema = json.loads(schema)
            avro.schema.parse(json.dumps(schema))
        except Exception as e:
            return False, AvroValidationError(e)
        return True, None

    @classmethod
    def validate_avro_schema_file(cls, file_path: str, quiet: bool = False) -> bool:
        """
        Checks if file contains a valid AVRO schema definition.
        :param file_path: Absolute path of file to check.
        :param quiet: Flag to suppress printouts of successful validations.
        :return: True if validation is successful. False otherwise.
        """
        with open(file_path, 'rb') as fp:
            success, e = cls.validate_schema_definition(schema=fp.read())
            if not success:
                print(f"FAIL: file {os.path.basename(file_path)} is not valid.\n\t{e}")
            elif not quiet:
                print(f"SUCCESS: AVRO schema in file {os.path.basename(file_path)} is valid.")
        return success

    @classmethod
    def validate_all_in_folder(cls, folder_path: str, include_str: str | None = None, avro_extension: str = AVRO_EXT,
                               quiet: bool = False) -> int:
        """
        Recursively searches for files with the specified extension `avro_extension` and checks if each contains a valid
        AVRO schema definition.
        :param folder_path: Absolute path of folder to check.
        :param include_str: If specified, only files including this expression will be checked (case-sensitive).
        :param avro_extension: Extension of files to be validated.
        :param quiet: Flag to suppress printouts of successful validations.
        :return: Number of successful validations.
        """
        schema_files = []
        for folder, _, files in os.walk(folder_path):
            schema_files += [os.path.join(folder, f) for f in files if os.path.splitext(f)[1] == avro_extension]

        success_count = 0
        for f in schema_files:
            if include_str and include_str not in f:
                continue
            success_count += cls.validate_avro_schema_file(file_path=f, quiet=quiet)
        return success_count


if __name__ == '__main__':

    def parse_args() -> argparse.Namespace:
        """
        Parse input arguments.
        :return: Namespace class object containing parsed arguments.
        """
        parser = argparse.ArgumentParser(prog="avro-schema-validator",
                                         description="Check validity of Avro schema definition files.")
        parser.add_argument('-p', '--path', dest='path', type=str, default=os.getcwd(),
                            help=' '.join(["Absolute path of file or folder with AVRO schema definition(s) to be",
                                           "validated. If not specified, defaults to current working directory."]))
        parser.add_argument('-f', '--filter', dest='include', type=str, default='',
                            help=' '.join(["Expression to restrict the files to be validated when scanning a",
                                           "directory; only files that contain the expression will be validated.",
                                           "If the argument passed in --path is a directory and --filter is not",
                                           "specified, all files will be validated. If the path specified is a"
                                           "file, --filter is ignored."]))
        parser.add_argument('-q', '--quiet', dest='quiet', action='store_true',
                            help="If passed when scanning a folder, all successful validations are omitted.")

        return parser.parse_args()


    def run(arguments: dict):
        """Run validator"""
        print("STARTING...")
        path = arguments.get('path')
        include = arguments.get('include')
        quiet = arguments.get('quiet') or False
        if path and os.path.isfile(path):
            AvroValidator.validate_avro_schema_file(file_path=path, quiet=quiet)
        elif path and os.path.isdir(path):
            success_count = AvroValidator.validate_all_in_folder(folder_path=path, include_str=include, quiet=quiet)
            print(f"{success_count} files successfully validated")
        else:
            print(f"ERROR Invalid path: {path}")
        print("ALL DONE!")

    args = parse_args()
    run(vars(args))
