from avro.field import AvroField


class GenericConnector:
    def __init__(self, **nargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

    def get_columns(self, **nargs) -> list[AvroField]:
        pass


class InvalidMapperException(Exception):
    pass

