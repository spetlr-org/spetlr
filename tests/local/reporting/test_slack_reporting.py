"""
This test exists both in the 'local' and in the 'cluster' folder.
The tests are not completely equal.
The reason is that the slack notifier should work both in a job and outside one.
"""
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from atc.reporting import SlackNotifier


class SlackNotifierTests(unittest.TestCase):
    httpd: HTTPServer
    serve_thread: threading.Thread
    hook_url: str

    class HTTPHandler(BaseHTTPRequestHandler):
        called = False
        called_data = {}

        def do_POST(self):
            SlackNotifierTests.HTTPHandler.called = True
            data_string = self.rfile.read(int(self.headers["Content-Length"]))

            self.send_response(200)
            self.end_headers()

            SlackNotifierTests.HTTPHandler.called_data = json.loads(data_string)

    @classmethod
    def setUpClass(cls) -> None:
        cls.httpd = HTTPServer(("localhost", 0), cls.HTTPHandler)
        cls.serve_thread = threading.Thread(target=cls.httpd.serve_forever)
        cls.serve_thread.start()
        cls.hook_url = f"http://localhost:{cls.httpd.server_port}"
        print(f"Serving webhook at {cls.hook_url}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()

    def setUp(self) -> None:
        self.HTTPHandler.called = False
        self.HTTPHandler.called_data = {}

    def test_01_info_webhook(self):
        slack = SlackNotifier(self.hook_url)

        slack.notify_info("my nice message")

        self.assertTrue(self.HTTPHandler.called)
        self.assertIn("text", self.HTTPHandler.called_data)
        self.assertRegex(
            self.HTTPHandler.called_data["text"], "message was sent from databricks"
        )
        self.assertRegex(self.HTTPHandler.called_data["text"], "test_01_info_webhook")
        self.assertRegex(self.HTTPHandler.called_data["text"], "my nice message")
        print(self.HTTPHandler.called_data["text"])

    def test_02_exc_webhook(self):
        slack = SlackNotifier(self.hook_url)

        try:
            1 / 0
        except ZeroDivisionError:
            slack.notify_exc()
            pass

        self.assertTrue(self.HTTPHandler.called)
        self.assertIn("text", self.HTTPHandler.called_data)
        self.assertRegex(
            self.HTTPHandler.called_data["text"],
            "An exception has occurred in databricks",
        )
        self.assertRegex(self.HTTPHandler.called_data["text"], "test_02_exc_webhook")
        print(self.HTTPHandler.called_data["text"])
