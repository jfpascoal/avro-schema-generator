from avro_tools.avro_type import AvroType


class AvroField:
    def __init__(self, name: str, typ: AvroType, nullable: bool):
        self._name = name
        self._type = typ
        self._nullable = nullable

    def __str__(self) -> str:
        return f"{self._name}: {self._type.name()}{' (optional)' if self._nullable else ''}"

    def to_dict(self) -> dict:
        if self._nullable:
            return {
                "name": self._name,
                "type": [
                    "null",
                    self._type.obj()
                ],
                "default": None
            }
        else:
            return {
                "name": self._name,
                "type": self._type.obj()
            }
