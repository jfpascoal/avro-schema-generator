import argparse
import json
import os
import sys

from configuration import Configuration, ConfigProperties
from connectors.sql_server.connector import SqlServerConnector
from connectors.csv.connector import CsvConnector
from avro_tools.generator import AvroGenerator

# Supported connectors
CONNECTORS_MAP = {
    "sqlserver": SqlServerConnector,
    "csv": CsvConnector
}


def parse_args() -> dict:
    """
    Parse input arguments when script is called from command line.
    :return: Dictionary with parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="avro-schema-generator",
        description="Generate AVRO schemas for Kafka Connect from the source database."
    )
    parser.add_argument('--connector', type=str, dest=ConfigProperties.CONNECTOR)
    parser.add_argument('--mapper', type=str, dest=ConfigProperties.MAPPER)
    parser.add_argument('--system', type=str, dest=ConfigProperties.DB_SYSTEM, default=None)
    parser.add_argument('--tables', type=str, dest=ConfigProperties.TABLES, default=None)
    parser.add_argument('--server', type=str, dest=ConfigProperties.DB_SERVER, default=None)
    parser.add_argument('--database', type=str, dest=ConfigProperties.DB_NAME, default=None)
    parser.add_argument('--schema', type=str, dest=ConfigProperties.DB_SCHEMA, default=None)
    parser.add_argument('--csv', type=str, dest=ConfigProperties.CSV_PATH, default=None)
    parser.add_argument('--out', type=str, dest=ConfigProperties.OUTPUT_DIR, default=os.getcwd())
    parser.add_argument('--nullable', type=bool, dest=ConfigProperties.ALL_NULLABLE, default=False)
    return vars(parser.parse_args())


def write_schema_to_file(avro_schema: dict, output_path: str, indent: int = 4, **kwargs) -> None:
    """
    Writes an AVRO schema to a file in the defined location.
    :param avro_schema: AVRO schema to write.
    :param output_path: Location of output file.
    :param indent: indentation size in JSON file.
    :return: None.
    """
    file_name = f"{avro_schema['namespace']}_{avro_schema['name']}".replace('.', '_') + '.avsc'
    file_path = os.path.join(output_path, file_name)
    with open(file_path, 'w') as fh:
        json.dump(avro_schema, fh, indent=indent, **kwargs)


def run(config: Configuration):
    connector = CONNECTORS_MAP.get(config.connector)
    if not connector:
        print(f"Invalid connector: {config.connector}")
        sys.exit()
    with connector(config) as connector:
        # if TABLES is specified, generate avro schemas for those tables; else, get list of existing tables using
        # connector and generate avro schema for each identified table.
        if config.connector_tables:
            table_names = config.connector_tables.split(',')
            tables = [(config.db_schema, table) for table in table_names]
        else:
            tables = connector.get_tables(config)

        for table in tables:
            table_name = table[1]
            avro_gen = AvroGenerator(connector)
            avro_schema = avro_gen.get_schema(table=table, avro_schema_name=table_name, config=config)
            write_schema_to_file(avro_schema=avro_schema, output_path=config.avro_output_path)


if __name__ == '__main__':
    args = parse_args() if len(sys.argv) > 1 else {}
    run(config=Configuration(props=args))

    sys.exit()
