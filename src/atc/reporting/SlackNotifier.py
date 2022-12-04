import sys
import time
import traceback
from datetime import datetime
from textwrap import dedent

import requests

from atc.exceptions import NoRunId

from .JobReflection import JobReflection


class SlackNotifier:
    """Send exceptions and notifications to slack webhooks
    while including stacktraces and links back to the job results.
    """

    def __init__(self, *webhookurl: str):
        self.webhookurls = list(webhookurl)

    def add_webhook_url(self, url: str):
        self.webhookurls.append(url)

    def notify_info(self, message: str = None, _stack_skip=1):
        """Send a notification to a webhook with the following example contents:
        "
        *A message was sent from your job <YOUR JOB NAME HERE>*

        Sent at 2022-11-20 01:03:17

        <YOUR MESSAGE HERE (IF ANY)>

        Caller info:
        ```
          File "/databricks/python_shell/scripts/PythonShell.py", line 29, in <module>
            launch_process()
        ...
        <STACK TRACE>
        ...
          File "/reporting/test_slack_reporting.py", line 53, in test_01_info_webhook
            slack.notify_info()
        ```
        <https://adb.../run/496|Go To Job Results>
        "
        """
        called_from = "".join(traceback.format_stack()[:-_stack_skip])
        try:
            job_name = JobReflection.get_job_name()
            text = f"*A message was sent from your job {job_name}*\n"
        except NoRunId:
            text = "*A message was sent from databricks*\n"

        text += f"\nSent at {self._slack_now()}\n"

        if message:
            text += f"\n{message}\n"

        text += f"\nCaller info:\n```\n{called_from}\n```\n"

        self._add_link_and_publish(text)

    def notify_exc(self):
        """Send a message about an ongoing exception
        to a webhook with the following example contents:
        "
        *An exception has occurred in your job <YOUR JOB NAME HERE>*

        The error occurred at 2022-11-20 01:03:17

        Traceback:
        ```
        Traceback (most recent call last):
          File "/reporting/test_slack_reporting.py", line 68, in test_02_exc_webhook
            1 / 0
        ZeroDivisionError: division by zero

        ```
        <https://adb-.../run/496|Go To Job Results>
        "
        """
        if sys.exc_info() == (None, None, None):
            return self.notify_info(
                message="SlackNotifier.notify_exc()"
                " was called with no active exception.",
                _stack_skip=2,
            )

        try:
            job_name = JobReflection.get_job_name()
            text = f"*An exception has occurred in your job {job_name}*\n"
        except NoRunId:
            text = "*An exception has occurred in databricks*\n"

        text += dedent(
            f"""The error occurred at {self._slack_now()}

        Traceback:
        ```
        {traceback.format_exc()}
        ```
        """
        )

        self._add_link_and_publish(text)

    def _slack_now(self):
        timestamp = int(time.time())
        date_format = "{date_long_pretty} {time_secs}"
        alt_text = str(datetime.now())
        return f"<!date^{timestamp}^{date_format}|{alt_text}>"

    def _add_link_and_publish(self, text: str):
        try:
            text += f"\n<{JobReflection.get_job_results_url()}|Go To Job Results>\n"
        except NoRunId:
            pass

        # send the whole thing off to the webhooks
        for url in self.webhookurls:
            res = requests.post(url=url, json={"text": text})
            if res.status_code != 200:
                # not raising an exception here makes this function more safe to call
                print(
                    "WARNING: Slack Notifier received "
                    f"unexpected status code {res.status_code}"
                )
