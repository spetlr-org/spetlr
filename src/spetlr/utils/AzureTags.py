import json
from typing import Dict

from spetlr.spark import Spark


class AzureTags:
    """Represents easy access to the azure resource tags of the workspace."""

    def __init__(self):
        tags_json = Spark.get().conf.get(
            "spark.databricks.clusterUsageTags.clusterAllTags"
        )
        self._tags: Dict[str, str] = {
            i["key"]: i["value"] for i in json.loads(tags_json)
        }

    def keys(self):
        return self._tags.keys()

    def __getattr__(self, attr) -> str:
        # only called when self.attr doesn't exist
        return self._tags[attr]

    def asDict(self) -> Dict[str, str]:
        return self._tags.copy()
