import argparse
import configparser
import json
import os
import sys

from enum import StrEnum

from connectors.sql_server.connector import SqlServerConnector
from avro.schema import AvroGenerator


class ConfigProperties(StrEnum):
    DB_CONNECTOR = 'DB_CONNECTOR'
    MAPPER = 'MAPPER'
    DB_SERVER = 'DB_SERVER'
    DB_NAME = 'DB_NAME'
    DB_SCHEMA = 'DB_SCHEMA'
    TABLES = 'TABLES'
    NAMESPACE = 'NAMESPACE'
    OUTPUT_DIR = 'OUTPUT_DIR'
    ALL_NULLABLE = 'ALL_NULLABLE'


CONNECTORS_MAP = {
    "SQLServer": SqlServerConnector
}


def parse_args() -> dict:
    parser = argparse.ArgumentParser(
        prog="avro-schema-generator",
        description="Generate AVRO schemas for Kafka Connect from the source database."
    )
    parser.add_argument('--connector', type=str, dest=ConfigProperties.DB_CONNECTOR)
    parser.add_argument('--mapper', type=str, dest=ConfigProperties.MAPPER)
    parser.add_argument('--server', type=str, dest=ConfigProperties.DB_SERVER)
    parser.add_argument('--database', type=str, dest=ConfigProperties.DB_NAME)
    parser.add_argument('--schema', type=str, dest=ConfigProperties.DB_SCHEMA)
    parser.add_argument('--out', type=str, dest=ConfigProperties.OUTPUT_DIR, default=os.getcwd())
    parser.add_argument('--nullable', type=bool, dest=ConfigProperties.ALL_NULLABLE, default=False)
    return vars(parser.parse_args())


def get_config() -> dict:
    parser = configparser.ConfigParser()
    parser.read(os.path.join(__file__, os.path.pardir, '.config'))
    config = parser['DEFAULT']
    return {k.upper(): config[k] for k in config}


def write_schema_to_file(schema: dict, output_path: str):
    file_name = f"{schema['namespace']}_{schema['name']}".replace('.', '_') + '.avsc'
    file_path = os.path.join(output_path, file_name)
    with open(file_path, 'w') as fh:
        json.dump(schema, fh, indent=4)


def run():
    connector = CONNECTORS_MAP.get(args[ConfigProperties.DB_CONNECTOR])
    if not connector:
        print(f"Invalid connector: {args[ConfigProperties.DB_CONNECTOR]}")
        sys.exit()
    with connector(args[ConfigProperties.DB_SERVER], args[ConfigProperties.DB_NAME],
                   args[ConfigProperties.MAPPER]) as connector:
        for table in args[ConfigProperties.TABLES].split(','):
            avro_gen = AvroGenerator(connector)
            schema = avro_gen.get_schema(namespace=args[ConfigProperties.NAMESPACE],
                                         schema_name=table, table_name=table,
                                         db_schema=args[ConfigProperties.DB_SCHEMA],
                                         all_nullable=args[ConfigProperties.ALL_NULLABLE])
            write_schema_to_file(schema=schema, output_path=args.get(ConfigProperties.OUTPUT_DIR) or os.getcwd())


if __name__ == '__main__':
    if len(sys.argv) > 1:
        args = parse_args()
    else:
        args = get_config()

    if args:
        run()
    else:
        print("ERROR: No configuration provided.")

    sys.exit()
