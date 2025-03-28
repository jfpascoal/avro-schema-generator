from avro_tools.avro_type import *
from connectors.snowflake.data_types import SnowflakeTypes


JDBC_DATATYPE_MAP = {
    SnowflakeTypes.BOOLEAN: AvroBoolean,
    SnowflakeTypes.FLOAT: AvroFloat,
    SnowflakeTypes.NUMBER: AvroDecimal,
    SnowflakeTypes.TEXT: AvroString,
    SnowflakeTypes.TIMESTAMP: AvroTimestampMillis
}