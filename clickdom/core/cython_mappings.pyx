import datetime as dt
import re

cpdef str val_to_str(val):
    if type(val) in [int, float, str]:
        return '{},'.format(val)
    if type(val) == list:
        return '{},'.format(str(val))
    if val is None:
        return 'NULL'
    if type(val) == dt.date:
        return "'{}',".format(val.strftime('%Y-%m-%d'))
    if type(val) == dt.datetime:
        return "'{}',".format(val.strftime('%Y-%m-%d %H:%M:%S'))


cdef str tuple_to_bytes(req_tuple):
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