from csv import DictReader

from avro.field import AvroField
from avro.types import AvroDecimal
from configuration import Configuration
from connectors.generic_connector import GenericConnector, InvalidMapperException
from connectors.snowflake.type_mappers import JDBC_DATATYPE_MAP as SNOWFLAKE_JDBC_MAPPER
from connectors.sql_server.type_mappers import DEBEZIUM_DATATYPE_MAP as SQLSERVER_DEBEZIUM_MAPPER
from connectors.sql_server.type_mappers import JDBC_DATATYPE_MAP as SQLSERVER_JDBC_MAPPER


class Column:
    class Headers:
        TABLE_NAME = 'TABLE_NAME'
        TABLE_SCHEMA = 'TABLE_SCHEMA'
        ORDINAL_POSITION = 'ORDINAL_POSITION'
        COLUMN_NAME = 'COLUMN_NAME'
        DATA_TYPE = 'DATA_TYPE'
        PRECISION = 'NUMERIC_PRECISION'
        SCALE = 'NUMERIC_SCALE'
        NULLABLE = 'IS_NULLABLE'


    def __init__(self, **values):
        self._table_name = values.get(self.Headers.TABLE_NAME)
        self._table_schema = values.get(self.Headers.TABLE_SCHEMA)
        self._ordinal_position = int(values.get(self.Headers.ORDINAL_POSITION))
        self._column_name = values.get(self.Headers.COLUMN_NAME)
        self._data_type = values.get(self.Headers.DATA_TYPE)
        self._numeric_precision = int(values.get(self.Headers.PRECISION))\
            if values.get(self.Headers.PRECISION)\
            else None
        self._numeric_scale = int(values.get(self.Headers.SCALE))\
            if values.get(self.Headers.SCALE)\
            else None
        self._is_nullable = values.get(self.Headers.NULLABLE).upper() in ('YES', 'TRUE', '1')

    @property
    def table_name(self):
        return self._table_name

    @property
    def table_schema(self):
        return self._table_schema

    @property
    def ordinal_position(self):
        return self._ordinal_position

    @property
    def column_name(self):
        return self._column_name

    @property
    def data_type(self):
        return self._data_type

    @property
    def numeric_precision(self):
        return self._numeric_precision

    @property
    def numeric_scale(self):
        return self._numeric_scale

    @property
    def is_nullable(self):
        return self._is_nullable


class CsvConnector(GenericConnector):

    TYPE_MAPPER = {
        "sqlserver": {
            "debezium": SQLSERVER_DEBEZIUM_MAPPER,
            "jdbc": SQLSERVER_JDBC_MAPPER
        },
        "snowflake": {
            "jdbc": SNOWFLAKE_JDBC_MAPPER
        }
    }

    def __init__(self, config: Configuration):
        super().__init__()
        self._csv_path = config.connector_csv_path
        db_system = config.db_system
        mapper = config.connector_mapper

        if db_system and mapper:
            try:
                self._mapper = self.TYPE_MAPPER[db_system][mapper]
            except KeyError as e:
                raise InvalidMapperException(str(e))

    def __enter__(self) -> GenericConnector:
        """
        Method executed when the connector is called as a context manager.
        Reads the CSV file and composes the list of tables defined therein as an instance attribute (_tables).
        The _tables attribute is a two layer dictionary. The first layer contains the db schemas; the second layer
        contains the tables; the values in the second layer are lists of Column instances that correspond to the
        table columns defined in the CSV file.
        """
        self._tables = {}
        with open(self._csv_path, newline='') as csv:
            csv_reader = DictReader(csv)
            for row in csv_reader:
                column = Column(**row)
                if column.table_schema in self._tables:
                    if column.table_name in self._tables[column.table_schema]:
                        self._tables[column.table_schema][column.table_name].append(column)
                    else:
                        self._tables[column.table_schema].update({column.table_name: [column]})
                else:
                    self._tables.update({column.table_schema: {column.table_name: [column]}})
        return self

    def __exit__(self, *args):
        pass

    def get_tables(self, config: Configuration) -> list:
        """
        Returns list of tables included in the CSV file. If the database schema is specified in the configuration
        properties, only the tables belonging to that schema are included. Otherwise, all tables defined in the
        CSV are included.
        :param config: Configuration properties.
        :return: List of tuples - (db_schema, table_name).
        """
        tables = []
        db_schema = config.db_schema

        for schema in self._tables:
            if schema != db_schema:
                continue
            for table in self._tables[schema]:
                tables.append((schema, table))
        return tables

    def get_columns(self, table: tuple[str, str], config: Configuration) -> list[AvroField]:
        """
        Returns list of AvroField's that correspond to the columns in the specified table.
        :param table: Tuple with DB schema and table name.
        :param config: Configuration properties.
        :return: List of AvroField instances.
        """
        db_schema = table[0]
        table_name = table[1]
        all_nullable = config.avro_all_nullable

        table_columns = self._tables[db_schema][table_name]

        # Sort columns by ordinal position
        table_columns.sort(key=lambda x: x.ordinal_position)

        avro_columns = []
        for col in table_columns:
            if self._mapper[col.data_type] is AvroDecimal:
                avro_type = AvroDecimal(precision=col.numeric_precision, scale=col.numeric_scale)
            else:
                avro_type = self._mapper[col.data_type]()
            avro_columns.append(
                AvroField(name=col.column_name,
                          typ=avro_type,
                          nullable=all_nullable or col.is_nullable)
            )
        return avro_columns
