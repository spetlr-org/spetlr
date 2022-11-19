import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from atc.reporting import SlackNotifier


class SlackNotifierTests(unittest.TestCase):
    class HTTPHandler(BaseHTTPRequestHandler):
        called = False
        called_data = {}

        def do_POST(self):
            SlackNotifierTests.HTTPHandler.called = True
            data_string = self.rfile.read(int(self.headers["Content-Length"]))

            self.send_response(200)
            self.end_headers()

            SlackNotifierTests.HTTPHandler.data = json.loads(data_string)

    def test_01_info_webhook(self):
        self.HTTPHandler.called = False
        self.HTTPHandler.called_data = {}

        with HTTPServer(("localhost", 0), self.HTTPHandler) as httpd:
            threading.Thread(target=httpd.serve_forever).start()

            slack = SlackNotifier(f"http://{httpd.server_name}:{httpd.server_port}/")

            # print(f"http://{httpd.server_name}:{httpd.server_port}/")
            # Call the WebHookNotifier here
            slack.notify_info()

            httpd.shutdown()
            self.assertTrue(self.HTTPHandler.called)
            self.assertIn("text", self.HTTPHandler.called_data)
            self.assertRegexpMatches(
                self.HTTPHandler.called_data["text"], "message was sent from databricks"
            )
            self.assertRegexpMatches(
                self.HTTPHandler.called_data["text"], "test_01_info_webhook"
            )

    def test_02_exc_webhook(self):
        self.HTTPHandler.called = False
        self.HTTPHandler.called_data = {}

        with HTTPServer(("localhost", 0), self.HTTPHandler) as httpd:
            threading.Thread(target=httpd.serve_forever).start()

            slack = SlackNotifier(f"http://{httpd.server_name}:{httpd.server_port}/")

            # print(f"http://{httpd.server_name}:{httpd.server_port}/")
            # Call the WebHookNotifier here
            try:
                1 / 0
            except ZeroDivisionError:
                slack.notify_exc()
                pass

            httpd.shutdown()
            self.assertTrue(self.HTTPHandler.called)
            self.assertIn("text", self.HTTPHandler.called_data)
            self.assertRegexpMatches(
                self.HTTPHandler.called_data["text"],
                "exception has occurred in your job Testing Run",
            )
            self.assertRegexpMatches(
                self.HTTPHandler.called_data["text"], "test_02_exc_webhook"
            )
