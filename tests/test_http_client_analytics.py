import unittest
from unittest.mock import patch

import requests

import abcp_client
import b24_client


class FakeResponse:
    def __init__(self, *, status_code: int, json_data=None, text: str = "") -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


class HttpClientAnalyticsTests(unittest.TestCase):
    def test_abcp_fetch_page_logs_success(self) -> None:
        fake_response = FakeResponse(status_code=200, json_data={"items": [{"userId": 1}]})

        class FakeSession:
            def get(self, *_args, **_kwargs):
                return fake_response

        with (
            patch.object(abcp_client, "_HTTP", FakeSession()),
            patch.object(abcp_client, "_wait_rate_limit", lambda: None),
            patch.object(abcp_client, "_mark_request_complete", lambda: None),
            patch.object(abcp_client, "with_retries", lambda fn, *, retries, backoff: fn()),
            patch.object(abcp_client, "record_http_transaction") as analytics_mock,
        ):
            payload = abcp_client._fetch_page(skip=0, limit=10)

        self.assertEqual(payload["items"][0]["userId"], 1)
        self.assertEqual(analytics_mock.call_count, 1)
        self.assertTrue(analytics_mock.call_args.kwargs["success"])
        self.assertEqual(analytics_mock.call_args.kwargs["provider"], "ABCP")

    def test_b24_call_logs_failure(self) -> None:
        fake_response = FakeResponse(
            status_code=400,
            json_data={"error": "ERROR", "error_description": "bad request"},
        )

        class FakeSession:
            def post(self, *_args, **_kwargs):
                return fake_response

        with (
            patch.object(b24_client, "_HTTP", FakeSession()),
            patch.object(b24_client, "REQUESTS_RETRIES", 0),
            patch.object(b24_client, "record_http_transaction") as analytics_mock,
        ):
            with self.assertRaises(b24_client.B24CallError):
                b24_client._call("crm.contact.add", {"fields": {"NAME": "Alice"}})

        self.assertEqual(analytics_mock.call_count, 1)
        self.assertFalse(analytics_mock.call_args.kwargs["success"])
        self.assertEqual(analytics_mock.call_args.kwargs["provider"], "Bitrix24")


if __name__ == "__main__":
    unittest.main()
