from typing import Any, Dict, List

from pypika import Query, Table

from clickdom.core.client import CoreClient
from clickdom.orm.abstract import ValidatedField


class TableException(ValueError):
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


class ClickhouseTable:

    def __init__(self):
        self.table_name = None

    def _validate(self, data: Dict, raise_exc=True):
        cls_inst = {key: value for (key, value) in self.__class__.__dict__.items()
                    if is_serializer(key, value)}
        validated_data, errors = validate_cls_instance(cls_inst, data, raise_exc)
        return validated_data

    def insert(self, client: CoreClient, data: dict):
        table = getattr(self, '__tablename__')
        if not table:
            raise TableException('No __tablename__ defined on table')
        data = self._validate(data)
        insert_data = tuple(data.values())
        client.execute('INSERT INTO {} VALUES'.format(table), insert_data)

    def filter(self, client: CoreClient, **kwargs):
        table = getattr(self, '__tablename__')
        if not table:
            raise TableException('No __tablename__ defined on table')

    def insert_many(self, client: CoreClient, data: List[dict]):
        pass


if __name__ == '__main__':
    from clickdom.orm.types import UInt8

    class MyTable(ClickhouseTable):
        __tablename__ = 'LOL'
        uint_field = UInt8()

    core = CoreClient('http://localhost:8123/')
    table = MyTable()
    #table.insert(core, {'uint_field': 8})
    table.filter(core, uint_field=8)