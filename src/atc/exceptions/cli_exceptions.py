from atc.exceptions import AtcException


class AtcCliException(AtcException):
    pass


class AtcCliCheckFailed(AtcCliException):
    pass
