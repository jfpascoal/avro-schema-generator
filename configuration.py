import configparser
import os


class ConfigProperties:
    CONNECTOR = 'connector'
    MAPPER = 'connector_mapper'
    CSV_PATH = 'connector_csv_path'
    TABLES = 'connector_tables'
    DB_SYSTEM = 'db_system'
    DB_SERVER = 'db_server'
    DB_NAME = 'db_name'
    DB_SCHEMA = 'db_schema'
    NAMESPACE = 'avro_namespace'
    OUTPUT_DIR = 'avro_output_path'
    ALL_NULLABLE = 'avro_all_nullable'


class Configuration:
    _CONFIG_PATH = os.path.join(os.path.curdir, ".config")

    def __init__(self, props: dict | None = None):
        if not props:
            props = self.parse_config(self._CONFIG_PATH)

        if not props.get(ConfigProperties.CONNECTOR) or not props.get(ConfigProperties.MAPPER):
            raise InvalidConfigurationError(
                [p for p in [ConfigProperties.CONNECTOR, ConfigProperties.MAPPER] if not props.get(p)]
            )

        self.connector = props.get(ConfigProperties.CONNECTOR)
        self.connector_mapper = props.get(ConfigProperties.MAPPER)
        self.connector_csv_path = props.get(ConfigProperties.CSV_PATH) or None
        self.connector_tables = props.get(ConfigProperties.TABLES) or None
        self.db_system = props.get(ConfigProperties.DB_SYSTEM) or None
        self.db_server = props.get(ConfigProperties.DB_SERVER) or None
        self.db_name = props.get(ConfigProperties.DB_NAME) or None
        self.db_schema = props.get(ConfigProperties.DB_SCHEMA) or None
        self.avro_namespace = props.get(ConfigProperties.NAMESPACE) or None
        self.avro_output_path = props.get(ConfigProperties.OUTPUT_DIR) or os.getcwd()
        self.avro_all_nullable = str(props.get(ConfigProperties.ALL_NULLABLE)).upper() in ['Y', 'T', 'YES', 'TRUE']\
                                 or False

    @staticmethod
    def parse_config(path: str) -> dict:
        parser = configparser.ConfigParser(allow_no_value=True)
        with open(path, 'r') as fh:
            parser.read_file(fh)
        config = parser['DEFAULT']
        return {k: config[k] or None for k in config}


class InvalidConfigurationError(Exception):
    def __init__(self, missing_properties: list[str]):
        self.msg = f"Missing configuration property/ies: {', '.join(missing_properties)}."

    def __str__(self):
        return f"ERROR {self.msg}"
