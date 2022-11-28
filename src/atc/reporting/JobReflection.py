import json
from functools import lru_cache
from typing import Dict

from requests import HTTPError

from atc.db_auto import getDbApi
from atc.exceptions import NoDbUtils, NoRunId
from atc.functions import init_dbutils


class JobReflection:
    @classmethod
    @lru_cache
    def get_job_details(cls) -> Dict:
        try:
            # this strange construct reaches beyond the pyspark api layer
            # and into undocumented structures in order to retrieve the details
            # of the current run.
            # if you know a better way, please add it here.
            return json.loads(
                init_dbutils()
                .notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
                .toJson()
            )
        except (AttributeError, NoDbUtils):
            raise NoRunId()

    @classmethod
    @lru_cache
    def get_job_api_details(cls) -> Dict:
        run_id = cls.get_current_run_id()
        db_api = getDbApi()

        try:
            return db_api.jobs.get_run(run_id=run_id)
        except HTTPError:
            raise NoRunId("DbApi returned http error.")

    @classmethod
    def get_job_results_url(cls) -> str:
        return cls.get_job_api_details()["run_page_url"]

    @classmethod
    def get_job_name(cls) -> str:
        return cls.get_job_api_details()["run_name"]

    @classmethod
    @lru_cache
    def get_current_run_id(cls) -> int:
        details = cls.get_job_details()
        try:
            # the first part is set in a task orchestration job
            # the second one is set in a plain old run
            run = details.get("currentRunId", None) or details["rootRunId"]
            return int(run["id"])  # convert to int for safety
        except (TypeError, KeyError):
            raise NoRunId()
