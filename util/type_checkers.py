import decimal
from ast import literal_eval

from dateutil.parser import parse as dt_parse


def str_to_type(s):
    try:
        k = literal_eval(s)
        return type(k)
    except:
        return type(s)


def is_timestamp(value: str) -> bool:
    try:
        dt_parse(value).timestamp()
        return True
    except:
        return False


def is_decimal(value: str) -> bool:
    s_int = __ignore_parse_exception()(decimal.Decimal)
    if s_int(value) is None:
        return False

    return True


def is_int(value: str) -> bool:
    s_int = __ignore_parse_exception()(int)
    if s_int(value) is None:
        return False

    return True


def is_float(value: str) -> bool:
    s_int = __ignore_parse_exception()(float)
    if s_int(value) is None:
        return False

    return True


def is_blank(string):
    val = str(string)
    return not (val and val.strip())


def is_not_blank(string):
    val = str(string)
    return bool(val and val.strip())


def is_complex(value: str) -> bool:
    s_int = __ignore_parse_exception()(complex)
    if s_int(value) is None:
        return False

    return True

def __ignore_parse_exception(default_value=None):
    def dec(function):
        def _dec(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except:
                return default_value

        return _dec

    return dec
