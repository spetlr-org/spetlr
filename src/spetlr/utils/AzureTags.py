import json

from spetlr.spark import Spark


class AzureTags:
    """Represents easy access to the azure resource tags of the workspace."""

    _tags = None

    @classmethod
    def init(cls):
        if cls._tags is None:
            tags_json = Spark.get().conf.get(
                "spark.databricks.clusterUsageTags.clusterAllTags"
            )
            cls._tags = {i["key"]: i["value"] for i in json.loads(tags_json)}

    @classmethod
    def keys(cls):
        cls.init()
        return cls._tags.keys()

    @classmethod
    def __getattr__(cls, attr):
        # only called when self.attr doesn't exist
        cls.init()
        return cls._tags[attr]
