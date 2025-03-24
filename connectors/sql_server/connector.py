import pyodbc

from avro_tools.field import AvroField
from avro_tools.avro_type import AvroDecimal
from configuration import Configuration
from connectors.generic_connector import GenericConnector, InvalidMapperException
from connectors.sql_server.type_mappers import DEBEZIUM_DATATYPE_MAP, JDBC_DATATYPE_MAP


class SqlServerConnector(GenericConnector):
    DRIVER = "{ODBC Driver 17 for SQL Server}"
    DB_SYSTEM = 'sqlserver'

    TYPE_MAPPER = {
        DB_SYSTEM: {
            "debezium": DEBEZIUM_DATATYPE_MAP,
            "jdbc": JDBC_DATATYPE_MAP
        }
    }

    def __init__(self, config: Configuration):
        super().__init__()

        self._server = config.db_server
        self._database = config.db_name
        try:
            self._mapper = self.TYPE_MAPPER[self.DB_SYSTEM][config.connector_mapper]
        except KeyError as e:
            raise InvalidMapperException(str(e))

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

    def get_columns(self, table: tuple[str], config: Configuration) -> list[AvroField]:
        """
        Generate AVRO definition of columns in the specified table.
        :param table: Tuple with DB schema and name of table.
        :param config: Configuration properties.
        :return: List of AvroField instances corresponding to the columns in the specified table.
        """
        db_schema = table[0]
        table_name = table[1]
        all_nullable = config.avro_all_nullable

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

