from typing import Any, Dict, List

from pypika import Query, Table

from clickdom.core.client import CoreClient
from clickdom.orm.abstract import ValidatedField


class ORMTableError(ValueError):
    pass


def is_serializer(key: str, value: Any):
    return isinstance(value, ValidatedField) and not key.startswith('_')


def validate_cls_instance(cls_inst: Dict[str, ValidatedField], data: Dict, raise_exc: bool):
    validated_data, errors = {}, {}
    for k, v in cls_inst.items():
        try:
            val = v.validate(v, data.get(k))
            validated_data[k] = val
        except ValueError as e:
            if raise_exc:
                raise e
            errors[k] = str(e)
    return validated_data, errors


def get_table_name(cls):
    table = getattr(cls, '__table__')
    if not table:
        raise ORMTableError('Model does not reference a Clickhouse table')
    return table


def column_values(data: Dict):
    fmt_tuple = '({})'
    fmt_values = ', '.join([k for k in data.keys()])
    return fmt_tuple.format(fmt_values)


def retrieve_values(cls, data: Dict):
    str_repr = []
    for k, v in data.items():
        type_cls = cls.__class__.__dict__.get(k)
        str_val = type_cls._clickhouse_repr(v)
        str_repr.append(str_val)
    return tuple(str_repr)


class ClickhouseTable:

    def __init__(self, **kwargs):
        self._data = self._validate(kwargs)

    def _validate(self, data: Dict, raise_exc=True):
        cls_inst = {key: value for (key, value) in self.__class__.__dict__.items()
                    if is_serializer(key, value)}
        validated_data, errors = validate_cls_instance(cls_inst, data, raise_exc)
        return validated_data

    def insert(self, client: CoreClient):
        query_fmt = 'INSERT INTO {table} VALUES'
        table = get_table_name(self)
        values = retrieve_values(self, self._data)
        print(values)
        query = query_fmt.format(table=table)
        c.execute(query, values)

    def filter(self, client: CoreClient, **kwargs):
        table = get_table_name(self)

    def insert_many(self, client: CoreClient, data: List[dict]):
        pass


if __name__ == '__main__':
    from clickdom.orm.types import UInt8, Array
    from clickdom.core.client import CoreClient

    c = CoreClient('http://localhost:8123/')

    class MyTable(ClickhouseTable):
        __table__ = 'LOL'
        array = Array(array_type=str)
        uint_field = UInt8()

    m = MyTable(uint_field=12, array=['Hello', 'World'])
    m.insert(c)