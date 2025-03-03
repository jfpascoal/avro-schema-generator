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
        self._numeric_precision = int(values.get(self.Headers.PRECISION)) \
            if values.get(self.Headers.PRECISION) \
            else None
        self._numeric_scale = int(values.get(self.Headers.SCALE)) \
            if values.get(self.Headers.SCALE) \
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
