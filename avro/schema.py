from connectors.generic_connector import GenericConnector


class AvroGenerator:
    def __init__(self, connector: GenericConnector):
        self._connector = connector
        self._dict = {}

    def get_schema(self, namespace: str, schema_name: str, **nargs) -> dict:
        self._dict = {
            "type": "record",
            "name": schema_name,
            "namespace": namespace,
            "fields": [column.to_dict() for column in self._connector.get_columns(**nargs)]
        }
        return self._dict

