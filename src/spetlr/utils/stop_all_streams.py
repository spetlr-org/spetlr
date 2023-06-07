from spetlr.spark import Spark


def stop_all_streams():
    """
    Stops all streams

    NB: This function will interfere with active streaming
        if tests is parallelized, consider creation a function
        that only stops streaming set up in the test class

    """
    for stream in Spark.get().streams.active:
        print(f'Stopping the stream "{stream.name}"')
        stream.stop()
        stream.awaitTermination()
