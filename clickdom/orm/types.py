import datetime as dt

from clickdom.orm.abstract import ValidatedField
from clickdom.core.utils import datetime_to_string, date_to_string

_ORM_MAPPING = {
    dt.datetime: datetime_to_string,
    dt.date: date_to_string,
}


class DateTime(ValidatedField):
    pass


class Date(ValidatedField):
    pass


class UInt8(ValidatedField):
    click_house_types = ['UInt8', ]

    def _validate_one(self, value):
        if not isinstance(value, int):
            raise ValueError('Invalid UInt8')
        return value

    def validate(self, instance, value):
        return self._validate_one(value)

    def _clickhouse_repr(self, value):
        return str(value)


class UInt16(ValidatedField):
    click_house_types = ['UInt16', ]

    def _validate_one(self, value):
        if not isinstance(value, int):
            raise ValueError('Invalid UInt8')
        return value

    def validate(self, instance, value):
        return self._validate_one(value)

    def _clickhouse_repr(self, value):
        return str(value)


class Array(ValidatedField):
    click_house_types = ['Array', ]

    def __init__(self, array_type=None):
        super().__init__()
        self._array_type = array_type

    def _validate_one(self, value):
        if not isinstance(value, (list, set)):
            raise ValueError('Array types must be a list or a set')
        obj_types = set()
        for item in value:
            obj_types.add(type(item))
        if len(obj_types) > 1:
            raise ValueError('Array types must contain single type')
        return value

    def validate(self, instance, value):
        return self._validate_one(value)

    def _clickhouse_repr(self, value):
        res = []
        type = self._array_type
        conversion_func = _ORM_MAPPING.get(type, str)
        for v in value:
            res.append(conversion_func(v))
        return res


class Nullable(ValidatedField):
    click_house_types = ['Nullable']

    def __init__(self, nullable_type=None):
        super().__init__()
        self._nullable_type = nullable_type

    def _validate_one(self, value):
        if not value:
            return None
        return value

    def validate(self, instance, value):
        return self._validate_one(value)


class Tuple(ValidatedField):
    click_house_types = ['Tuple']

    def __init__(self, tuple_types):
        super().__init__()
        self._tuple_types = tuple_types

    def _validate_one(self, value):
        raise NotImplementedError

    def validate(self, instance, value):
        raise NotImplementedError


if __name__ == '__main__':
    pass