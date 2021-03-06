import datetime as dt
import re

from clickdom.core.utils import datetime_to_string, date_to_string


def val_to_str(val) -> str:
    if type(val) in [int, float, str]:
        return '{},'.format(val)
    if type(val) == list:
        return '{},'.format(str(val))
    if val is None:
        return 'NULL'
    if type(val) == dt.date:
        return datetime_to_string(val)
    if type(val) == dt.datetime:
        return datetime_to_string(val)


def tuple_to_bytes(req_tuple):
    command = '('
    tuple_list = list(req_tuple)
    for val in tuple_list:
        if type(val) == tuple:
            command += tuple_to_bytes(val)
        else:
            command += val_to_str(val)
    command += '),'
    return command


def to_bytes(*args):
    val_list = []
    for arg in args:
        command = ""
        for val in arg:
            if type(val) == tuple:
                command += tuple_to_bytes(val)
        command = command.replace(',)', ')')
        val_list.append(command)
    return ','.join(val_list).encode()


def to_date(val):
    y, m, d = val.strip("'").split('-')
    return dt.date(year=int(y), month=int(m), day=int(d))


def to_datetime(val):
    if val == "'0000-00-00 00:00:00'":
        return None
    return dt.datetime.strptime(val, "'%Y-%m-%d %H:%M:%S'")


def to_nothing(val):
    return None


def to_string(val):
    return val.strip("'")


CLICK_TO_PY = {
    'UInt8': int,
    'UInt16': int,
    'UInt32': int,
    'UInt64': int,
    'Int8': int,
    'Int16': int,
    'Int32': int,
    'Int64': int,
    'Float32': float,
    'Float64': float,
    'Decimal': float,
    'Date': to_date,
    'DateTime': to_datetime,
    'String': to_string,
    'FixedString': str,
    'UUID': str,
    'Nothing': to_nothing,
}


def resolve_low_cardinality(ch_type: str, val: str):
    real_type = re.search(r'LowCardinality\((?P<name>[^\)]+)', ch_type).group('name')
    return CLICK_TO_PY[real_type](val)


def resolve_nullable(ch_type: str, val: str):
    if val == 'NULL':
        return None
    real_type = re.search(r'Nullable\((?P<name>[^\)]+)', ch_type).group('name')
    return CLICK_TO_PY[real_type](val)


def resolve_array(ch_type: str, val: str):
    real_type = re.search(r'Array\((?P<name>[^\)]+)', ch_type).group('name')
    str_list = val.replace('[', '').replace(']', '').split(',')
    if len(str_list) > 0:
        if real_type in CLICK_TO_PY:
            return [CLICK_TO_PY[real_type](v) for v in str_list]
    return []


def resolve_tuple(ch_type: str, val: str):
    cleaned_tuple = ch_type.replace('Tuple(', '', 1)[:-1].split(',')
    cleaned_types = [t.lstrip() for t in cleaned_tuple]
    cleaned_values = val[1:-1].split(',')
    vals = []
    for i in zip(cleaned_types, cleaned_values):
        val_type, tup_val = i
        if val_type in CLICK_TO_PY:
            vals.append(CLICK_TO_PY[val_type](tup_val))
        elif val_type.startswith('Nullable'):
            vals.append(resolve_nullable(val_type, tup_val))
    return tuple(vals)
