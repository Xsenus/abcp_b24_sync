import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import request_analytics


class RequestAnalyticsTests(unittest.TestCase):
    def test_record_http_transaction_writes_daily_log_and_masks_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch.object(request_analytics, "REQUEST_ANALYTICS_ENABLED", True),
                patch.object(request_analytics, "REQUEST_ANALYTICS_DIR", tmp_dir),
                patch.object(request_analytics, "REQUEST_ANALYTICS_MAX_BODY_CHARS", 128),
            ):
                request_analytics.record_http_transaction(
                    provider="Bitrix24",
                    operation="crm.contact.add",
                    http_method="POST",
                    url="https://example.bitrix24.ru/rest/1/token/crm.contact.add",
                    request_payload={"userpsw": "secret", "fields": {"NAME": "Alice"}},
                    response_payload={"result": 101},
                    status_code=200,
                    success=True,
                    attempt=1,
                )

                files = list(Path(tmp_dir).glob("request_analytics_*.log"))
                self.assertEqual(len(files), 1)

                line = files[0].read_text(encoding="utf-8").strip()
                payload = json.loads(line)
                self.assertEqual(
                    payload["url"],
                    "https://example.bitrix24.ru/rest/***/***/crm.contact.add",
                )
                self.assertEqual(payload["request"]["userpsw"], "***")
                self.assertEqual(payload["outcome"], "positive")
                self.assertTrue(payload["success"])

    def test_record_http_transaction_truncates_long_strings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch.object(request_analytics, "REQUEST_ANALYTICS_ENABLED", True),
                patch.object(request_analytics, "REQUEST_ANALYTICS_DIR", tmp_dir),
                patch.object(request_analytics, "REQUEST_ANALYTICS_MAX_BODY_CHARS", 10),
            ):
                request_analytics.record_http_transaction(
                    provider="ABCP",
                    operation="users.list",
                    http_method="GET",
                    url="https://example.com/api",
                    request_payload={"filter": "X" * 50},
                    response_payload=None,
                    success=False,
                    error="Y" * 50,
                )

                file_path = next(Path(tmp_dir).glob("request_analytics_*.log"))
                payload = json.loads(file_path.read_text(encoding="utf-8").strip())
                self.assertIn("<truncated", payload["request"]["filter"])
                self.assertIn("<truncated", payload["error"])


if __name__ == "__main__":
    unittest.main()
