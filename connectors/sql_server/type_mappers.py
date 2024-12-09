from avro.types import *
from connectors.sql_server.data_types import SqlServerTypes


DEBEZIUM_DATATYPE_MAP = {
    SqlServerTypes.BIGINT: AvroLong,
    SqlServerTypes.BINARY: AvroBytes,
    SqlServerTypes.BIT: AvroBoolean,
    SqlServerTypes.CHAR: AvroString,
    SqlServerTypes.DATE: AvroDate,
    SqlServerTypes.DATETIME: AvroTimestampMillis,
    SqlServerTypes.DATETIME2: AvroTimestampMillis,
    SqlServerTypes.DATETIMEOFFSET: AvroString,
    SqlServerTypes.DECIMAL: AvroDecimal,
    SqlServerTypes.FLOAT: AvroDouble,
    SqlServerTypes.INT: AvroInt,
    SqlServerTypes.MONEY: AvroDecimal,
    SqlServerTypes.NCHAR: AvroString,
    SqlServerTypes.NUMERIC: AvroDecimal,
    SqlServerTypes.NVARCHAR: AvroString,
    SqlServerTypes.NTEXT: AvroString,
    SqlServerTypes.REAL: AvroFloat,
    SqlServerTypes.SMALLDATETIME: AvroTimestampMillis,
    SqlServerTypes.SMALLINT: AvroInt,
    SqlServerTypes.SMALLMONEY: AvroDecimal,
    SqlServerTypes.TEXT: AvroString,
    SqlServerTypes.TIME: AvroTimeMillis,
    SqlServerTypes.TINYINT: AvroInt,
    SqlServerTypes.UNIQUEIDENTIFIER: AvroString,
    SqlServerTypes.VARBINARY: AvroBytes,
    SqlServerTypes.VARCHAR: AvroString,
    SqlServerTypes.XML: AvroString
}


JDBC_DATATYPE_MAP = {
    SqlServerTypes.BIGINT: AvroLong,
    SqlServerTypes.BINARY: AvroBytes,
    SqlServerTypes.BIT: AvroInt,
    SqlServerTypes.CHAR: AvroString,
    SqlServerTypes.DATE: AvroDate,
    SqlServerTypes.DATETIME: AvroTimestampMillis,
    SqlServerTypes.DATETIME2: AvroTimestampMillis,
    SqlServerTypes.DATETIMEOFFSET: AvroTimestampMillis,
    SqlServerTypes.DECIMAL: AvroDecimal,
    SqlServerTypes.FLOAT: AvroDouble,
    SqlServerTypes.INT: AvroInt,
    SqlServerTypes.MONEY: AvroDecimal,
    SqlServerTypes.NCHAR: AvroString,
    SqlServerTypes.NUMERIC: AvroDecimal,
    SqlServerTypes.NVARCHAR: AvroString,
    SqlServerTypes.NTEXT: AvroString,
    SqlServerTypes.REAL: AvroFloat,
    SqlServerTypes.SMALLDATETIME: AvroTimestampMillis,
    SqlServerTypes.SMALLINT: AvroInt,
    SqlServerTypes.SMALLMONEY: AvroDecimal,
    SqlServerTypes.TEXT: AvroString,
    SqlServerTypes.TIME: AvroTimeMillis,
    SqlServerTypes.TINYINT: AvroInt,
    SqlServerTypes.UNIQUEIDENTIFIER: AvroString,
    SqlServerTypes.VARBINARY: AvroBytes,
    SqlServerTypes.VARCHAR: AvroString,
    SqlServerTypes.XML: AvroString
}
