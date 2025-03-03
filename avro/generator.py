from configuration import Configuration
from connectors.generic_connector import GenericConnector


class AvroGenerator:
    """
    Abstracts the generation of an AVRO schema as a dictionary.
    """

    def __init__(self, connector: GenericConnector):
        self._connector = connector
        self._dict = {}

    def get_schema(self, table: tuple[str, str], avro_schema_name: str, config: Configuration) -> dict:
        """
        Generates AVRO schema for specified table.
        :param table: Tuple containing DB schema and table name.
        :param avro_schema_name: Name of AVRO schema.
        :param config: Configuration properties.
        :return: Dictionary with AVRO schema.
        """
        self._dict = {
            "type": "record",
            "name": avro_schema_name,
            "namespace": config.avro_namespace,
            "fields": [column.to_dict() for column in self._connector.get_columns(table, config)]
        }
        return self._dict
