from atc.spark import Spark


def stop_all_streams():
    for stream in Spark.get().streams.active:
        print(f'Stopping the stream "{stream.name}"')
        stream.stop()
        stream.awaitTermination()
