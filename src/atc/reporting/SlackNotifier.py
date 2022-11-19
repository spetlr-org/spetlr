import sys
import time
import traceback
from datetime import datetime

import requests

from ..atc_exceptions import NoRunId
from .JobReflection import JobReflection
from .WebHookNotifier import WebHookNotifier


class SlackNotifier(WebHookNotifier):
    def __init__(self, webhookurl: str):
        self.webhookurl = webhookurl

    def notify_info(self, message: str = None, _stack_skip=1):
        called_from = "".join(traceback.format_stack()[:-_stack_skip])
        try:
            job_name = JobReflection.get_job_name()
            job_url = JobReflection.get_job_results_url()
        except NoRunId:
            job_url = job_name = ""

        if job_name:
            text = f"*A message was sent from your job {job_name}*\n"
        else:
            text = "*A message was sent from databricks*\n"

        text += (
            "\nSent at <!date^"
            + str(int(time.time()))
            + "^{date_long_pretty} {time_secs}|"
            + str(datetime.now())
            + ">\n"
        )

        text += f"\n{message}\n"

        text += f"\nCaller info:\n```\n{called_from}\n```\n"

        if job_url:
            text += f"<{job_url}|Go To Job Results>\n"

        # send the whole thing off to the webhook
        res = requests.post(url=self.webhookurl, json={"text": text})
        if res.status_code != 200:
            # not raising an exception here makes this function more safe to call
            print(
                "WARNING: Slack Notifier received "
                f"unexpected status code {res.status_code}"
            )

    def notify_exc(self):
        if sys.exc_info() == (None, None, None):
            return self.notify_info(
                message="SlackNotifier.notify_exc()"
                " was called with no active exception.",
                _stack_skip=2,
            )

        try:
            job_name = JobReflection.get_job_name()
            job_url = JobReflection.get_job_results_url()
        except NoRunId:
            job_url = job_name = ""

        if job_name:
            text = f"*An exception has occurred in your job {job_name}*\n"
        else:
            text = "*An exception has occurred in databricks*\n"

        text += (
            "\nThe error occurred at <!date^"
            + str(int(time.time()))
            + "^{date_long_pretty} {time_secs}|"
            + str(datetime.now())
            + ">\n"
        )

        text += f"\nTraceback:\n```\n{traceback.format_exc()}\n```\n"

        if job_url:
            text += f"<{job_url}|Go To Job Results>\n"

        # send the whole thing off to the webhook
        res = requests.post(url=self.webhookurl, json={"text": text})
        if res.status_code != 200:
            # not raising an exception here makes this function more safe to call
            print(
                "WARNING: Slack Notifier received "
                f"unexpected status code {res.status_code}"
            )
