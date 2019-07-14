

def datetime_to_string(val):
    return "'{}',".format(val.strftime('%Y-%m-%d %H:%M:%S'))


def date_to_string(val):
    return "'{}',".format(val.strftime('%Y-%m-%d'))