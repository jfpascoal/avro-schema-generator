# Generic type
class AvroType:
    _NAME = None

    @classmethod
    def name(cls) -> str:
        return cls._NAME

    @classmethod
    def obj(cls) -> str:
        return cls.name()


# Primitive types
class AvroBoolean(AvroType):
    _NAME = "boolean"


class AvroBytes(AvroType):
    _NAME = "bytes"


class AvroDouble(AvroType):
    _NAME = "double"


class AvroFloat(AvroType):
    _NAME = "float"


class AvroInt(AvroType):
    _NAME = "int"


class AvroLong(AvroType):
    _NAME = "long"


class AvroNull(AvroType):
    _NAME = "null"


class AvroString(AvroType):
    _NAME = "string"


# Logical types
class AvroLogicalType(AvroType):
    _PRIMITIVE_TYPE = None

    def __init__(self, **props):
        self._props = props

    def obj(self) -> dict:
        return {
            "type": self._PRIMITIVE_TYPE,
            **self._props,
            "logicalType": self._NAME
        }


class AvroDate(AvroLogicalType):
    _NAME = "date"
    _PRIMITIVE_TYPE = AvroInt.name()


class AvroDecimal(AvroLogicalType):
    _NAME = "decimal"
    _PRIMITIVE_TYPE = AvroBytes.name()

    def __init__(self, precision: int, scale: int):
        super().__init__(scale=scale, precision=precision)


class AvroTimestampMillis(AvroLogicalType):
    _NAME = "timestamp-millis"
    _PRIMITIVE_TYPE = AvroLong.name()


class AvroTimeMillis(AvroLogicalType):
    _NAME = "time-millis"
    _PRIMITIVE_TYPE = AvroInt.name()
