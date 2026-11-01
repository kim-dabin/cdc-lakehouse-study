"""In-process ASGI checks; no port, Spark, MinIO or HTTP client dependency."""
import asyncio
import datetime
import decimal
import importlib.util
import json
import unittest
from unittest.mock import patch

HAS_FASTAPI = importlib.util.find_spec("fastapi") is not None
if HAS_FASTAPI:
    from tools.explorer.app import main


@unittest.skipUnless(HAS_FASTAPI, "FastAPI가 설치되지 않았습니다.")
class APITests(unittest.TestCase):
    def request(self, method, path, body=None, query=b""):
        async def run():
            messages = []
            payload = json.dumps(body).encode() if body is not None else b""
            sent = False

            async def receive():
                nonlocal sent
                if sent:
                    return {"type": "http.disconnect"}
                sent = True
                return {"type": "http.request", "body": payload, "more_body": False}

            async def send(message):
                messages.append(message)

            await main.app({"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
                            "method": method, "scheme": "http", "path": path, "raw_path": path.encode(),
                            "query_string": query, "root_path": "", "headers": [(b"content-type", b"application/json")],
                            "client": ("127.0.0.1", 1), "server": ("localhost", 8000)}, receive, send)
            status = next(m["status"] for m in messages if m["type"] == "http.response.start")
            data = b"".join(m.get("body", b"") for m in messages if m["type"] == "http.response.body")
            return status, json.loads(data)
        return asyncio.run(run())

    def test_invalid_sql_never_opens_connection(self):
        with patch.object(main.spark, "_connect") as connect:
            status, data = self.request("POST", "/api/sql", {"sql": "USE lake.gold", "max_rows": 200})
            self.assertEqual(status, 400)
            self.assertIn("error", data)
            connect.assert_not_called()

    def test_row_count_validation(self):
        with patch.object(main.spark, "query") as query:
            for rows in (0, 501, True, "200"):
                status, data = self.request("POST", "/api/sql", {"sql": "SELECT 1", "max_rows": rows})
                self.assertEqual(status, 422)
            query.assert_not_called()

    def test_identifier_validation_before_query(self):
        with patch.object(main.spark, "query") as query:
            status, _ = self.request("GET", "/api/tables", query=b"ns=silver%3BDROP")
            self.assertEqual(status, 400)
            status, _ = self.request("GET", "/api/table/silver/a.b/files")
            self.assertEqual(status, 400)
            query.assert_not_called()

    def test_fixed_catalog_queries(self):
        result = {"columns": [], "rows": [], "truncated": False, "elapsed_ms": 1}
        with patch.object(main.spark, "query", return_value=result) as query:
            status, _ = self.request("GET", "/api/table/silver/refunds/files")
            self.assertEqual(status, 200)
            self.assertIn("lake.silver.refunds.files", query.call_args.args[0])

    def test_health_and_errors_redact_credentials(self):
        with patch.object(main, "_SECRETS", ("private-test-value",)), \
             patch.object(main.spark, "query", side_effect=RuntimeError("private-test-value")), \
             patch.object(main.storage, "health", side_effect=RuntimeError("private-test-value")):
            status, data = self.request("GET", "/api/health")
            self.assertEqual(status, 200)
            self.assertFalse(data["spark"]["ok"])
            self.assertFalse(data["minio"]["ok"])
            self.assertNotIn("private-test-value", json.dumps(data))
            self.assertIn("checked_at", data["spark"])
            status, data = self.request("POST", "/api/sql", {"sql": "SELECT 1"})
            self.assertEqual(status, 502)
            self.assertNotIn("private-test-value", json.dumps(data))

    def test_json_values(self):
        self.assertEqual(main.public(decimal.Decimal("1.20")), "1.20")
        self.assertEqual(main.public(datetime.date(2026, 1, 1)), "2026-01-01")
        self.assertEqual(main.public(b"\xff"), "base64:/w==")
        json.dumps(main.public([float("nan"), float("inf")]), allow_nan=False)
