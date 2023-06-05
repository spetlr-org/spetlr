class SpetlrException(Exception):
    pass


class SpetlrKeyError(KeyError):
    pass


class NoTableException(SpetlrException):
    value = "No table found!"
    pass


class UnkownPathException(SpetlrException):
    value = "Something went wrong during reading of path!"
    pass


class ColumnDoesNotExistException(SpetlrException):
    pass


class MoreThanTwoDataFramesException(SpetlrException):
    pass


class EhJsonToDeltaException(SpetlrException):
    pass


class NoSuchSchemaException(SpetlrException):
    pass


class FalseSchemaDefinitionException(SpetlrException):
    pass


class UnregisteredSchemaDefinitionException(SpetlrException):
    pass


class NoRunId(SpetlrException):
    pass


class NoDbUtils(SpetlrException):
    pass


class NoSuchValueException(SpetlrKeyError):
    pass


class MissingUpsertJoinColumns(SpetlrKeyError):
    value = "You must specify upsert_join_cols"
    pass


class OnlyUseInSpetlrDebugMode(SpetlrKeyError):
    value = "Only call this if the configurator is in debug"
    
class MissingEitherStreamLoaderOrHandle(SpetlrException):
    value = "StreamLoader requires either a handle or a loader as input."
    pass


class AmbiguousLoaderInput(SpetlrException):
    value = "StreamLoader requires either a handle or a loader as input."
    pass


class NotAValidStreamTriggerType(SpetlrException):
    value = "Trigger_type should either be {'availablenow', 'once', 'processingtime'}"
    pass


class NeedTriggerTimeWhenProcessingType(SpetlrException):
    value = "Trigger_time_seconds must be specified if trigger_type = 'processingtype'"
    pass


class UnknownStreamOutputMode(SpetlrException):
    value = (
        "Output mode should be one of the following ['complete', 'append', 'update']"
    )
    pass
