import datetime
from datetime import datetime as dt
from typing import List, Tuple

import py4j.protocol
import pyspark.sql.utils
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from atc.configurator.configurator import Configurator
from atc.spark import Spark

utc = datetime.timezone.utc


class EventHubCaptureExtractor:
    """Class to access eventhub capture files as table.

    The constructor argument should be given as a key that can be found in
    the Table Configurator, where the relevant table must be marked with
    format=avro and with a partitioning from the set of known partitions: y,m,d,h
    """

    path: str
    partitioning: str

    @classmethod
    def from_tc(cls, tbl_id: str):
        tc = Configurator()
        assert tc.table_property(tbl_id, "format") == "avro"
        return cls(
            path=tc.table_property(tbl_id, "path"),
            partitioning=tc.table_property(tbl_id, "partitioning"),
        )

    def __init__(self, path: str, partitioning: str):
        self.path = path
        self.partitioning = partitioning.lower()
        assert self.partitioning in ["ymd", "ymdh"]

    def _validate_timestamp(self, stamp: dt):
        """Check that the given timestamp is an edge
        between two capture periods according to the set partitioning."""
        stamp = stamp.astimezone(utc)
        if stamp.microsecond != 0:
            return False
        if stamp.second != 0:
            return False
        if stamp.minute != 0:
            return False
        if "h" not in self.partitioning and stamp.hour != 0:
            return False
        return True

    def _add_columns(self, df: DataFrame) -> DataFrame:
        # here we extract the partition description from the input filename
        # partitions are saved in folders like .../y=2022/m=09/d=23/h=02/...
        # there are many ways to extract these parts. We use a single regular expression
        # the parts are ordered by assumption (validated in manual tests)
        # but this ordering is not verified in order to prioritize performance
        df = df.withColumn(
            "_parts",
            f.expr('regexp_extract_all(input_file_name(),"[ymdh]=([0-9]+)/",1)'),
        )
        df = df.withColumn("y", f.element_at("_parts", 1).cast("INTEGER"))
        df = df.withColumn("m", f.element_at("_parts", 2).cast("INTEGER"))
        df = df.withColumn("d", f.element_at("_parts", 3).cast("INTEGER"))
        if "h" in self.partitioning:
            df = df.withColumn("h", f.element_at("_parts", 4).cast("INTEGER"))

        # unfortunately, regexp_extract_all and make_timestamp are not exposed
        # in the pyspark wrapper library
        df = df.withColumn(
            "pdate",
            f.expr(
                "make_timestamp(y,m,d,"
                + ("h," if "h" in self.partitioning else "0,")
                + '0,0,"UTC")'
            ),
        )
        df = df.drop("_parts")

        # for some bizarre reason, the built-in string timestamp uses
        # a localized date-time format and not a standardized one.
        # add a standardized timestamp
        df = df.withColumn(
            "EnqueuedTimestamp",
            f.to_timestamp(f.col("EnqueuedTimeUtc"), "M/d/yyyy h:mm:ss a"),
        )
        return df

    def _now_utc(self):
        # this method needs to be here so that I can override it in the unittest
        return dt.now(tz=utc)

    def _validated_slice_arguments(
        self, from_partition: dt = None, to_partition: dt = None
    ) -> Tuple[dt, dt]:
        from_partition = from_partition.astimezone(datetime.timezone.utc)

        if not self._validate_timestamp(from_partition):
            raise ValueError("from_partition is not a pure partition edge")

        if to_partition is None:
            # if the to_partition is not given, we set it to the end of
            to_partition = dt(self._now_utc().year + 1, 1, 1, tzinfo=utc)
        else:
            # to_partition was given by the caller, we need to validate it.
            to_partition = to_partition.astimezone(datetime.timezone.utc)

            if not self._validate_timestamp(to_partition):
                raise ValueError("to_partition is not a clean partition edge")

            # ensure that limits are ordered correctly
            if to_partition < from_partition:
                raise ValueError("Negative partition interval.")

        if to_partition == from_partition:
            raise ValueError("Requested to read over zero partitions.")

        return from_partition, to_partition

    def _break_into_partitioning_parts(self, fp: dt, tp: dt) -> List[str]:
        """
        data is grouped to use as few spark.read.load commands as possible
        while still respecting the given limits exactly.
        The slice is cut using the largest bite possible for each cut.
        """
        parts_to_load: List[str] = []

        # We now have a slice description [fp, tp)
        # we now take bites out of this slice until the slice is closed.
        # in each iteration we take the biggest bite that we can,
        while fp != tp and fp < self._now_utc():
            # consider a year-sized bite
            # does fp represent a lower edge of a year slice:
            if fp.month == 1 and fp.day == 1 and fp.hour == 0:
                # fp seems suitable, check tp
                year_end = fp.replace(year=fp.year + 1)
                if year_end <= tp:
                    # tp is far enough away. We can take a one-year bite!
                    parts_to_load.append(f"y={fp.year:04}/")
                    fp = year_end
                    continue

            # consider a month-sized bite
            if fp.day == 1 and fp.hour == 0:
                # fp seems suitable, check tp
                # no easier way to add a month afaik
                month_end = (fp + datetime.timedelta(days=32)).replace(day=1)
                if month_end <= tp:
                    # we can take a one-month bite!
                    parts_to_load.append(f"y={fp.year:04}/m={fp.month:02}/")
                    fp = month_end
                    continue

            # consider a day-sized bite
            if fp.hour == 0:
                # fp seems suitable, check tp
                day_end = fp + datetime.timedelta(days=1)
                if day_end <= tp:
                    # we can take a one-month bite!
                    parts_to_load.append(
                        f"y={fp.year:04}/m={fp.month:02}/d={fp.day:02}/"
                    )
                    fp = day_end
                    continue

            # consider an hour-sized bite
            if "h" in self.partitioning:
                # fp is always suitable due to argument validation
                hour_end = fp + datetime.timedelta(hours=1)
                # we don't need really need to check tp
                # because we know that fp!=tp and that fp<tp and that
                # both represent valid partitioning edges
                # but let's do it anyway
                if hour_end <= tp:
                    # we can take a one-hour bite!
                    parts_to_load.append(
                        f"y={fp.year:04}/m={fp.month:02}/"
                        f"d={fp.day:02}/h={fp.hour:02}/"
                    )
                    fp = hour_end
                    continue

            raise Exception("The loading logic failed. Contact the maintainer.")
        return parts_to_load

    def _load_union_of_parts(self, parts_to_load: List[str]) -> DataFrame:
        spark = Spark.get()
        df = None
        for part in parts_to_load:
            try:
                part_df = spark.read.format("avro").load(self.path + "/" + part)
            except (pyspark.sql.utils.AnalysisException, py4j.protocol.Py4JError):
                print(
                    f"WARNING: part {part} caused an analysis exception. "
                    "The partition is probably empty."
                )
                continue
            if df is None:
                df = self._add_columns(part_df)
            else:
                df = df.unionByName(self._add_columns(part_df))

        if df is None:
            raise Exception("The selected slice returned no data.")

        return df

    def read(
        self,
        from_partition: datetime.datetime = None,
        to_partition: datetime.datetime = None,
    ) -> DataFrame:
        r"""Read all data between the two limits.
        Supported usage:
            - set neither value, all files are read.
            - set from_partition only, (to_partition is assumed to be now)
            - set both from_partition and to_partition
        Given partition limits are designating the edges of partitions to read. Hence:
            - The from_partition is inclusive.
            - The to_partition is exclusive.
        If no to_partition is given, all data will be read, including the
        (usually only partially filled) current partition.
        """

        # internally there are only two real cases,
        # reading everything
        # or reading a slice.
        # first we handle the case of reading everything
        if from_partition is None:
            if to_partition is not None:
                raise ValueError("Use both limits or only 'from' limit.")
            else:
                # read all
                return self._add_columns(
                    Spark.get().read.format("avro").load(self.path)
                )

        # now we know that a slice will be read.

        # we need pure inputs. The timestamps have to point at partition edges
        from_partition, to_partition = self._validated_slice_arguments(
            from_partition, to_partition
        )

        # data is grouped to use as few spark.read.load commands as possible
        # while still respecting the given limits exactly
        parts_to_load = self._break_into_partitioning_parts(
            from_partition, to_partition
        )

        # we know that from_partition != to_partition
        # and the function above must return some partitions
        assert len(parts_to_load)

        return self._load_union_of_parts(parts_to_load)

    def get_partitioning(self):
        return list(self.partitioning)
