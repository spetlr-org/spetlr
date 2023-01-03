CREATE DATABASE IF NOT EXISTS {IncrementalExtractorDb}
LOCATION "{IncrementalExtractorDb_path}";

CREATE TABLE IF NOT EXISTS {IncrementalExtractorDummySource}
(
    id INTEGER,
    stringcol INTEGER,
    timecol TIMESTAMP
)
USING DELTA
LOCATION "{IncrementalExtractorDummySource_path}";

CREATE TABLE IF NOT EXISTS {IncrementalExtractorDummyTarget}
(
    id INTEGER,
    stringcol INTEGER,
    timecol TIMESTAMP
)
USING DELTA
LOCATION "{IncrementalExtractorDummyTarget_path}";
