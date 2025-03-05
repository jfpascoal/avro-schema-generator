from avro_tools.field import AvroField
from configuration import Configuration


class GenericConnector:
    """
    Defines abstract Connector and methods.
    """
    TYPE_MAPPER = {}

    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def get_tables(self, config: Configuration) -> list[str]:
        pass

    def get_columns(self, table: tuple[str, str], config: Configuration) -> list[AvroField]:
        pass


class InvalidMapperException(Exception):
    def __init__(self, mapper: str, valid: tuple = ()):
        self.msg = f"Data type mapper not recognized {mapper}." \
            + f"\nValid options: {valid}" if valid else ""

    def __str__(self):
        return f"ERROR {self.msg}"
