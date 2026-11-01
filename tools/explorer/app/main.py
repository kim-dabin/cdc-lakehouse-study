"""Explorer HTTP API and local, dependency-free browser UI."""
import base64
import dataclasses
import datetime
import decimal
import logging
import math
import os
from functools import wraps
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from . import spark, storage
from .paths import table_identifier

for name in ("pyhive", "boto3", "botocore", "thrift"):
    logging.getLogger(name).setLevel(logging.CRITICAL)

app = FastAPI(title="Lakehouse Explorer", docs_url=None, redoc_url=None, openapi_url=None)
_SECRETS = tuple(os.environ[k] for k in (
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "S3_ACCESS_KEY", "S3_SECRET_KEY")
    if os.environ.get(k))


def public(value):
    """JSON-safe values and credential redaction, including service errors."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        # 브라우저 숫자는 2^53을 넘는 정수를 정확히 담지 못한다(snapshot_id 등). 문자열로 보낸다.
        return value if abs(value) <= 2**53 - 1 else str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, dict):
        return {public(str(k)): public(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [public(v) for v in value]
    if dataclasses.is_dataclass(value):
        return public(dataclasses.asdict(value))
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        value = value.isoformat()
    elif isinstance(value, bytes):
        value = "base64:" + base64.b64encode(value).decode("ascii")
    elif isinstance(value, decimal.Decimal):
        value = str(value)
    else:
        value = str(value)
    for secret in _SECRETS:
        value = value.replace(secret, "[숨김]")
    return value


def respond(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            return JSONResponse(public(function(*args, **kwargs)))
        except ValueError as exc:
            return JSONResponse({"error": public(str(exc))}, status_code=400)
        except TimeoutError as exc:
            return JSONResponse({"error": public(str(exc))}, status_code=504)
        except Exception as exc:
            return JSONResponse({"error": public(str(exc))}, status_code=502)
    return wrapped


@app.exception_handler(RequestValidationError)
async def invalid_request(request, exc):
    return JSONResponse({"error": "입력값과 행 수를 확인해 주세요."}, status_code=422)


class SQLRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sql: str = Field(max_length=20_000)
    max_rows: int = Field(default=200, ge=1, le=spark.MAX_ROWS, strict=True)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/health")
@respond
def health():
    result = {}
    try:
        version = spark.query("SELECT version()", 1)
        result["spark"] = {"ok": True, "version": version["rows"][0][0]}
    except Exception as exc:
        result["spark"] = {"ok": False, "error": str(exc)}
    result["spark"]["checked_at"] = datetime.datetime.now(datetime.timezone.utc)
    try:
        storage.health()
        result["minio"] = {"ok": True}
    except Exception as exc:
        result["minio"] = {"ok": False, "error": str(exc)}
    result["minio"]["checked_at"] = datetime.datetime.now(datetime.timezone.utc)
    return result


@app.post("/api/sql")
@respond
def sql(body: SQLRequest):
    return spark.query(body.sql, body.max_rows)


@app.get("/api/namespaces")
@respond
def namespaces():
    return spark.query("SHOW NAMESPACES IN lake", spark.MAX_ROWS)


@app.get("/api/tables")
@respond
def tables(ns: str):
    return spark.query(f"SHOW TABLES IN {table_identifier(ns)}", spark.MAX_ROWS)


@app.get("/api/table/{ns}/{table}/{section}")
@respond
def table_section(ns: str, table: str, section: str):
    identifier = table_identifier(ns, table)
    statements = {
        "snapshots": f"SELECT * FROM {identifier}.snapshots ORDER BY committed_at DESC LIMIT 20",
        "files": f"SELECT content, file_path, file_format, record_count, file_size_in_bytes FROM {identifier}.files",
        "metadata-log": f"SELECT timestamp, file, latest_snapshot_id FROM {identifier}.metadata_log_entries ORDER BY timestamp DESC",
    }
    if section not in statements:
        raise ValueError("지원하지 않는 테이블 정보입니다.")
    return spark.query(statements[section], spark.MAX_ROWS)


@app.get("/api/files")
@respond
def files(prefix: str = Query(default="", max_length=1024),
          token: str | None = Query(default=None, max_length=8192)):
    return storage.list_files(prefix, token)


@app.get("/api/file")
@respond
def file(path: str = Query(max_length=2048)):
    return storage.preview(path)
