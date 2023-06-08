from spetlr.spark import Spark


def stop_all_streams():
    """
    Stops all streams
    """
    for stream in Spark.get().streams.active:
        print(f'Stopping the stream "{stream.name}"')
        stream.stop()
        stream.awaitTermination()
