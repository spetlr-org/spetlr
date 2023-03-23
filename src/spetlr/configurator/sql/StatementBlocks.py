from dataclasses import dataclass
from typing import Dict, List

ENUM_ASC = 1
ENUM_DESC = 2


@dataclass
class SortingCol:
    name: str
    ordering: int = ENUM_ASC


@dataclass
class ClusteredBy:
    clustering_cols: List[str]
    n_buckets: int = 0
    sorting: List[SortingCol] = None


@dataclass
class StatementBlocks:
    schema: str = None
    using: str = None
    options: Dict[str, str] = None
    partitioned_by: List[str] = None
    clustered_by: ClusteredBy = None
    location: str = None
    comment: str = None
    tblproperties: Dict[str, str] = None
    dbproperties: Dict[str, str] = None

    def get_simple_structure(self):
        object_details = {
            "path": self.location,
            "format": self.using.lower() if self.using else None,
            "options": self.options,
            "partitioned_by": self.partitioned_by,
            "comment": self.comment,
            "tblproperties": self.tblproperties,
        }

        if self.schema:
            self.schema = self.schema.strip()
            if self.schema[0] == "(":
                self.schema = self.schema[1:-1]
            object_details["schema"] = {"sql": self.schema}
            object_details["_raw_sql_schema"] = self.schema

        if self.clustered_by:
            object_details["clustered_by"] = {
                "cols": self.clustered_by.clustering_cols,
                "buckets": self.clustered_by.n_buckets,
            }
            if self.clustered_by.sorting:
                object_details["clustered_by"]["sorted_by"] = []
                for col in self.clustered_by.sorting:
                    object_details["clustered_by"]["sorted_by"].append(
                        dict(
                            name=col.name,
                            ordering=("ASC" if col.ordering == ENUM_ASC else "DESC"),
                        )
                    )

        return {k: v for k, v in object_details.items() if v is not None}
