import pyodbc

from connectors.generic_connector import GenericConnector, InvalidMapperException
from connectors.sql_server.type_mappers import DEBEZIUM_DATATYPE_MAP, JDBC_DATATYPE_MAP
from avro.field import AvroField
from avro.types import AvroDecimal


class SqlServerConnector(GenericConnector):
    DRIVER = "{ODBC Driver 17 for SQL Server}"

    def __init__(self, server: str, database: str, mapper: str):
        super().__init__()
        self._server = server
        self._database = database
        if mapper == "debezium":
            self._mapper = DEBEZIUM_DATATYPE_MAP
        elif mapper == "jdbc":
            self._mapper = JDBC_DATATYPE_MAP
        else:
            raise InvalidMapperException(f"Data type mapper not recognized: {mapper}")

    def __enter__(self):
        connection_string = (
            f"DRIVER={self.DRIVER};"
            + f"SERVER={self._server};"
            + f"DATABASE={self._database};"
            + "Trusted_connection=yes"
        )
        self._connection = pyodbc.connect(connection_string, autocommit=True, readonly=True)
        return self

    def __exit__(self, *args):
        self._connection.close()

    def get_columns(self, table_name: str, db_schema: str, all_nullable: bool = False) -> list[AvroField]:
        columns_dict = {}
        cursor = self._connection.cursor()
        rows = cursor.columns(table=table_name, schema=db_schema)

        for row in rows:
            if self._mapper[row.type_name] is AvroDecimal:
                avro_type = AvroDecimal(row.column_size, row.decimal_digits)
            else:
                avro_type = self._mapper[row.type_name]()
            columns_dict.update({row.ordinal_position: AvroField(
                    name=row.column_name,
                    typ=avro_type,
                    nullable=all_nullable or bool(row.nullable)
            )})

        i = 1
        columns = []
        while columns_dict:
            columns.append(columns_dict.pop(i))
            i += 1

        return columns

