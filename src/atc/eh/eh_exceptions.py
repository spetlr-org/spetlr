from atc.exceptions import AtcException


class AtcEhException(AtcException):
    pass


class AtcEhInitException(AtcEhException):
    pass


class AtcEhLogicException(AtcEhException):
    pass
