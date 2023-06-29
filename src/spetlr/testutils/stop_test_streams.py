from spetlr.configurator import Configurator
from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.spark import Spark


def stop_test_streams():
    """
    Stops all streams in the current SPETLR config session.

    This mean streams, that has a query name with the Configurator UUID
    """

    c = Configurator()
    if not c.is_debug():
        raise OnlyUseInSpetlrDebugMode()

    id_extension = c.get("ID")

    for stream in Spark.get().streams.active:
        if id_extension in stream.name:
            print(f'Stopping the stream "{stream.name}"')
            stream.stop()
            stream.awaitTermination()
