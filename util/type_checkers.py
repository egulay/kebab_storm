from dateutil.parser import parse as dt_parse


def is_timestamp(value: str) -> bool:
    if value is None or not str(value).strip():
        return False
    try:
        dt_parse(str(value)).timestamp()
        return True
    except (TypeError, ValueError, OverflowError):
        return False


def is_not_blank(string):
    return string is not None and bool(str(string).strip())
