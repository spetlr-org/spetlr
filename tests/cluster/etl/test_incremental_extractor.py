from datetime import timedelta
from typing import List

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.testing import DataframeTestCase, TestHandle
from spetlrtools.time import dt_utc

from spetlr.etl.extractors.incremental_extractor import IncrementalExtractor
from spetlr.utils import DataframeCreator


class IncrementalExtractorTests(DataframeTestCase):
    date_row1 = dt_utc(2021, 1, 1, 10, 50)  # 1st of january 2021, 10:50
    date_row2 = dt_utc(2021, 1, 1, 10, 55)  # 1st of january 2021, 10:55
    date_row2Inc = dt_utc(2021, 1, 1, 10, 56)  # 1st of january 2021, 10:56
    date_row3 = dt_utc(2021, 1, 1, 11, 00)  # 1st of january 2021, 11:00
    date_row4 = dt_utc(2021, 1, 5, 11, 00)  # 5th of january 2021, 11:00

    row1 = (1, "string1", date_row1)
    row2 = (2, "string2", date_row2)
    row2Inc = (22, "string2Inc", date_row2Inc)
    row3 = (3, "String3", date_row3)
    row4 = (4, "String4", date_row4)

    # Test data for empty target or source
    source1 = [row1, row2, row3]
    target1 = [row1, row2]

    # Test data for incremental extraction
    source1Inc = [row1, row2Inc, row3]
    target1Inc = [row1, row2]
    extract1Inc = [row2Inc, row3]

    # Test data for overlapping period
    sourceOverlap = [row2, row3, row4]
    targetOverlap = [row2, row3, row4]
    extractOverlap = [row2, row3, row4]

    # The columns of the tables
    dummy_columns: List[str] = ["id", "stringcol", "timecol"]

    # The schema of the tables
    dummy_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("stringcol", StringType(), True),
            StructField("timecol", TimestampType(), True),
        ]
    )

    def test_01_can_perform_incremental_from_empty_source(self):
        """Source is empty. Target has data. No data is read.


        Source has no data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |  |              |                  |

        Target has the following data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |1 | "string1"    | 01.01.2021 10:50 |
        |2| "string2"     | 01.01.2021 10:55 |

        So an empty dataframe is read:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |  |              |                  |

        """

        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, []
            )
        )

        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.target1
            )
        )

        extractor = IncrementalExtractor(
            handle_source=source_test_handle,
            handle_target=target_test_handle,
            time_col_source="timecol",
            time_col_target="timecol",
            dataset_key="source",
        )

        df_result = extractor.read()

        self.assertDataframeMatches(df_result, None, [])

    def test_02_can_perform_incremental_on_empty_target(self):
        """Source has data. Target is empty. All source data are read.

        Source has the following data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |1 | "string1"    | 01.01.2021 10:50 |
        |2| "string2"     | 01.01.2021 10:55 |
        |3 | "string3"    | 01.01.2021 11:00 |

        Target has no data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |  |              |                  |

        The following data (all source data) is read:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |1 | "string1"    | 01.01.2021 10:50 |
        |2| "string2"     | 01.01.2021 10:55 |
        |3 | "string3"    | 01.01.2021 11:00 |
        """
        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.source1
            )
        )

        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, []
            )
        )

        extractor = IncrementalExtractor(
            handle_source=source_test_handle,
            handle_target=target_test_handle,
            time_col_source="timecol",
            time_col_target="timecol",
            dataset_key="source",
        )

        df_extract = extractor.read()

        self.assertDataframeMatches(df_extract, None, self.source1)

    def test_03_can_extract_incremental(self):
        """
        Source has the following data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |1 | "string1"    | 01.01.2021 10:50 |
        |22| "string2inc" | 01.01.2021 10:56 |
        |3 | "string3"    | 01.01.2021 11:00 |


        Target has the following data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |1 | "string1"    | 01.01.2021 10:50 |
        |2| "string2"     | 01.01.2021 10:55 |

        So data from after 01.01.2021 10:55 should be read:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |22| "string2inc" | 01.01.2021 10:56 |
        |3 | "string3"    | 01.01.2021 11:00 |
        """
        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.source1Inc
            )
        )

        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.target1Inc
            )
        )

        extractor = IncrementalExtractor(
            handle_source=source_test_handle,
            handle_target=target_test_handle,
            time_col_source="timecol",
            time_col_target="timecol",
            dataset_key="source",
        )

        df_extract = extractor.read()

        self.assertDataframeMatches(df_extract, None, self.extract1Inc)

    def test_04_can_extract_with_overlap(self):
        """
        Source has the following data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |2 | "string2"    | 01.01.2021 10:55 |
        |3|  "string3"    | 01.01.2021 11:00 |
        |4 | "string4"    | 05.01.2021 11:00 |


        Target has the following data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |2 | "string2"    | 01.01.2021 10:55 |
        |3|  "string3"    | 01.01.2021 11:00 |
        |4 | "string4"    | 05.01.2021 11:00 |

        Since an overlap of 5 days id defined all data should have been read:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |2 | "string2"    | 01.01.2021 10:55 |
        |3|  "string3"    | 01.01.2021 11:00 |
        |4 | "string4"    | 05.01.2021 11:00 |
        """
        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.sourceOverlap
            )
        )

        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.targetOverlap
            )
        )

        extractor = IncrementalExtractor(
            handle_source=source_test_handle,
            handle_target=target_test_handle,
            time_col_source="timecol",
            time_col_target="timecol",
            dataset_key="source",
            overlap_period=timedelta(days=5),
        )

        df_extract = extractor.read()

        self.assertDataframeMatches(df_extract, None, self.extractOverlap)

    def test_05_can_extract_with_overlap_empty_target(self):
        """
        Source has the following data:
        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |2 | "string2"    | 01.01.2021 10:55 |
        |3|  "string3"    | 01.01.2021 11:00 |
        |4 | "string4"    | 05.01.2021 11:00 |

        Target has no data:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |  |              |                  |

        All data should been read:

        |id| stringcol    | timecol          |
        |--|--------------|------------------|
        |2 | "string2"    | 01.01.2021 10:55 |
        |3|  "string3"    | 01.01.2021 11:00 |
        |4 | "string4"    | 05.01.2021 11:00 |
        """
        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.sourceOverlap
            )
        )
        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, []
            )
        )
        extractor = IncrementalExtractor(
            handle_source=source_test_handle,
            handle_target=target_test_handle,
            time_col_source="timecol",
            time_col_target="timecol",
            dataset_key="source",
            overlap_period=timedelta(days=5),
        )
        df_extract = extractor.read()
        self.assertDataframeMatches(df_extract, None, self.extractOverlap)
