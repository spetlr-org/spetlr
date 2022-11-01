import json
import os
from datetime import datetime
from typing import Union

from deprecated import deprecated
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException

from atc import dbg
from atc.configurator.configurator import Configurator
from atc.eh.eh_exceptions import AtcEhInitException, AtcEhLogicException
from atc.eh.PartitionSpec import PartitionSpec
from atc.functions import init_dbutils
from atc.spark import Spark


@deprecated(
    reason="Use EventHubCaptureExtractor instead.",
)
class EventHubCapture:
    """Class to access eventhub capture files as table.

    The constructor argument should be given as a key that can be found in
    the Table Configurator, where the relevant table must be marked with
    format=avro and with a partitioning from the set of known partitions: y,m,d,h
    """

    max_partition: Union[PartitionSpec, None]
    name: str
    path: str
    format: str
    partitioning: str
    auto_create: bool

    @classmethod
    def from_tc(cls, id: str):
        tc = Configurator()
        return cls(
            name=tc.table_property(id, "name"),
            path=tc.table_property(id, "path"),
            format=tc.table_property(id, "format"),
            partitioning=tc.table_property(id, "partitioning"),
        )

    def __init__(
        self,
        name: str,
        path: str,
        format: str,
        partitioning: str,
        auto_create: bool = True,
    ):
        self.name = name
        self.path = path
        self.format = format
        self.partitioning = partitioning.lower()
        self.auto_create = auto_create

        assert self.format == "avro"
        self._validate_partitioning()
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

    def _discover_first_partition(self) -> PartitionSpec:
        """Add the first partition by discovering it from filesystem."""
        # raise if the table already has partitions
        if Spark.get().sql(f"SHOW PARTITIONS {self.name}").count():
            raise AtcEhLogicException("Table partitions already initialized.")

        dbutils = init_dbutils()
        partition = PartitionSpec()

        # discover each part
        for c in self.partitioning:
            # loop over items in path
            full_path = self.path + "/" + partition.as_path()
            for file_info in dbutils.fs.ls(full_path):
                if not (
                    file_info.name.startswith(c + "=") and file_info.name.endswith("/")
                ):
                    continue
                extraction = int(file_info.name[2:-1])
                previous = partition.__getattribute__(c)
                # keep only the lowest
                if previous is None or extraction < previous:
                    partition.__setattr__(c, extraction)

            # done looping over items.
            if partition.__getattribute__(c) is None:
                # There is probably no data, yet.
                raise AtcEhInitException(
                    f"unable to discover first partition at '{full_path}'"
                )

        # we have discovered all parts of the partition.
        Spark.get().sql(
            f"""ALTER TABLE {self.name} ADD PARTITION ({partition.as_sql_spec()})"""
        )

        # return the partition in path form so that the repair can continue
        return partition

    def _get_max_partition(self) -> PartitionSpec:
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
                if max_p is not None:
                    dbg(f"There is an old partition! {max_p}")
                    self.max_partition = PartitionSpec.from_path(max_p)
                # value will be None if there is no value
            except AnalysisException:
                # no such table. Better create one
                if self.auto_create:
                    self._create_table()
                else:
                    raise AtcEhInitException("external table does not exist")

        if self.max_partition is None:
            # reading partition metadata failed.
            # initialize first partition from disk.
            self.max_partition = self._discover_first_partition()

        return self.max_partition

    def _repair_partitioning(self):
        self._get_max_partition()

        # compare with current wall-clock to see if partitions are missing
        now = datetime.now()

        additions = []
        while self.max_partition.is_earlier_than_dt(now):
            # we know that the partition table is behind
            next_partition = self.max_partition.next()

            dbg(
                "we are behind on the partitions. "
                f"Try adding next {next_partition.as_path()}"
            )

            # check for existence of partition. If it does not exits,
            # databricks would otherwise try to create the folder which breaks
            # for read-only mounts.
            try:
                # Here we do not use dbutils because it does not have a meaningful
                # exception when the file does not exist. Also, it seems slower.
                # The only negative consequence is that the partitions will not
                # be updated over databricks-connect
                os.listdir("/dbfs" + self.path + "/" + next_partition.as_path())
                additions.append(f"PARTITION ({next_partition.as_sql_spec()})")

            except FileNotFoundError:
                pass

            self.max_partition = next_partition

        while additions:
            # We need to add partitions
            parts_per_batch = min(len(additions), 24)
            dbg(f"adding {parts_per_batch} partitions")

            # add partitions in batch to prevent an sql line with hundreds of parts
            batch = [additions.pop() for _ in range(parts_per_batch)]
            Spark.get().sql(f"ALTER TABLE {self.name} ADD " + " ".join(batch))
        else:
            dbg("no partitions to add")

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
