import json
import re
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException

from atc.config_master import TableConfigurator
from atc.eh.eh_exceptions import AtcEhInitException, AtcEhLogicException
from atc.functions import init_dbutils
from atc.spark import Spark


class EventHubCapture:
    """Class to access eventhub capture files as table.

    The constructor argument should be given as a key that can be found in
    the Table Configurator, where the relevant table must be marked with
    format=avro and with a partitioning from the set of known partitions: y,m,d,h
    """

    @classmethod
    def from_tc(cls, id: str):
        tc = TableConfigurator()
        return cls(
            name=tc.table_property(id, "name"),
            path=tc.table_property(id, "path"),
            format=tc.table_property(id, "format"),
            partitioning=tc.table_property(id, "partitioning"),
        )

    def __init__(self, name: str, path: str, format: str, partitioning: str):
        self.name = name
        self.path = path
        self.format = format.lower()
        self.partitioning = partitioning.lower()

        assert self.format == "avro"
        self._validate_partitioning()

        if "h" in self.partitioning:
            self.partition_delta = timedelta(hours=1)
        else:
            self.partition_delta = timedelta(days=1)

        self.max_partition = None

    def _validate_partitioning(self, partitioning: str = None):
        if not partitioning:
            partitioning = self.partitioning

        if partitioning not in ["ymd", "ymdh"]:
            raise ValueError("partitioning must be ymd or ymdh")

        return True

    def _create_table(self):
        """Create the external avro table"""
        avro_schema = {
            "namespace": "org.apache.hive",
            "name": "first_schema",
            "type": "record",
            "fields": [
                {"name": "SequenceNumber", "type": "long"},
                {"name": "Offset", "type": "string"},
                {"name": "EnqueuedTimeUtc", "type": "string"},
                {"name": "Body", "type": "string"},
            ],
        }
        Spark.get().sql(
            f"""
            CREATE TABLE {self.name}
            ({", ".join(f"{c} STRING" for c in self.partitioning)})
            STORED AS AVRO
            PARTITIONED BY ({",".join(self.partitioning)})
            LOCATION "{self.path}"
            TBLPROPERTIES ('avro.schema.literal'='{json.dumps(avro_schema)}');
        """
        )

    def _discover_first_partition(self):
        """Add the first partition by discovering it from filesystem."""
        # raise if the table already has partitions
        if Spark.get().sql(f"SHOW PARTITIONS {self.name}").count():
            raise AtcEhLogicException("Table partitions already initialized.")

        dbutils = init_dbutils()
        partition = {}
        partition_path = ""

        # discover each part
        for c in self.partitioning:
            # loop over items in path
            value = None
            for file_info in dbutils.fs.ls(self.path + "/" + partition_path):
                if not (
                    file_info.name.startswith(c + "=") and file_info.name.endswith("/")
                ):
                    continue
                extraction = file_info.name[2:-1]

                # keep only the lowest
                if value is None or int(extraction) < int(value):
                    value = extraction

            # done looping over items.
            if value is None:
                # There is probably no data, yet.
                raise AtcEhInitException("unable to discover first partition")

            if partition_path:
                partition_path += "/"
            partition_path += f"{c}={value}"
            partition[c] = value

        # we have discovered all parts of the partition.
        Spark.get().sql(
            f"""ALTER TABLE {self.name} ADD PARTITION ({
            ", ".join(f"{c}='{v}'" for c, v in partition.items())
            })"""
        )

        # return the partition in path form so that the repair can continue
        return partition_path

    def _get_max_partition(self):
        """get the current highest partition either
        1. from memory
        2. read the partition table
        3. initialize from disk
        """
        if self.max_partition is None:
            # memory access failed.
            # try reading partition table
            try:
                max_p = (
                    Spark.get()
                    .sql(f"SHOW PARTITIONS {self.name}")
                    .groupBy()
                    .agg(f.max("partition"))
                    .collect()
                )[0][0]
                # value will be None if there is no value
            except AnalysisException:
                # no such table. Better create one
                self._create_table()
                # new table has no partitions
                max_p = None

            if not max_p:
                # reading partition metadata failed.
                # initialize first partition from disk.
                max_p = self._discover_first_partition()

            # we got the partition in the form y=2022/m=04/d=04
            # extract the parts
            pat = re.compile("/".join(f"{c}=(?P<{c}>\\d+)" for c in self.partitioning))
            match = pat.match(max_p)
            self.max_partition = datetime(
                year=int(match.group("y")),
                month=int(match.group("m")),
                day=int(match.group("d")),
                hour=int(match.group("h")) if "h" in self.partitioning else 0,
            )
        return self.max_partition

    def _repair_partitioning(self):
        self._get_max_partition()

        # compare with current wall-clock to see if partitions are missing
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        if "h" not in self.partitioning:
            now = now.replace(hour=0)

        additions = []
        while self.max_partition < now:
            # we know that the partition table is behind
            next_partition = self.max_partition + self.partition_delta
            partition_spec = (
                f"y='{next_partition.year:04}', "
                f"m='{next_partition.month:02}', "
                f"d='{next_partition.day:02}'"
            )
            if "h" in self.partitioning:
                partition_spec += f", h='{next_partition.hour:02}'"
            additions.append(f"PARTITION ({partition_spec})")

        while additions:
            # We need to add partitions
            parts_per_batch = min(len(additions), 24)
            print(f"adding {parts_per_batch} partitions")

            # add partitions in batch to prevent an sql line with hundreds of parts
            batch = [additions.pop() for _ in range(parts_per_batch)]
            Spark.get().sql(f"ALTER TABLE {self.name} ADD " + " ".join(batch))
        else:
            pass
            # no partitions to add

    def read(self) -> DataFrame:
        self._repair_partitioning()
        return (
            Spark.get()
            .table(self.name)
            .withColumn(
                "pdate",
                f.expr(
                    'make_timestamp(y,m,d,h,0,0,"UTC")'
                    if "h" in self.partitioning
                    else 'make_timestamp(y,m,d,0,0,0,"UTC")'
                ),
            )
        )
