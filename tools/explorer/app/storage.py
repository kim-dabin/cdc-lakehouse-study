"""Read-only warehouse browsing with bounded object downloads."""
import io
import itertools
import json
import os
import threading
from contextlib import closing

from .paths import parse_path

BUCKET = os.getenv("WAREHOUSE_BUCKET", "warehouse")
PREVIEW_LIMIT = 64 * 1024 * 1024
_PREVIEWS = threading.BoundedSemaphore(1)


def client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
                        region_name=os.getenv("AWS_REGION", "us-east-1"),
                        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                                      connect_timeout=5, read_timeout=15, retries={"max_attempts": 0}))


def health():
    client().head_bucket(Bucket=BUCKET)


def list_files(prefix="", token=None):
    args = dict(Bucket=BUCKET, Prefix=prefix, Delimiter="/", MaxKeys=1000)
    if token:
        args["ContinuationToken"] = token
    response = client().list_objects_v2(**args)
    return {"prefix": prefix,
            "folders": [p["Prefix"] for p in response.get("CommonPrefixes", [])],
            "files": [{"key": item["Key"], "path": f"s3://{BUCKET}/{item['Key']}",
                       "size": item["Size"], "modified": item["LastModified"]}
                      for item in response.get("Contents", [])],
            "next_token": response.get("NextContinuationToken")}


def preview(path):
    bucket, key = parse_path(path, BUCKET)
    if not _PREVIEWS.acquire(blocking=False):
        raise RuntimeError("다른 파일을 읽고 있습니다. 잠시 후 다시 열어 주세요.")
    try:
        return _preview(bucket, key, path)
    finally:
        _PREVIEWS.release()


def _preview(bucket, key, path):
    s3 = client()
    head = s3.head_object(Bucket=bucket, Key=key)
    size = head["ContentLength"]
    result = {"path": path, "size": size, "modified": head["LastModified"], "kind": "metadata"}
    if size > PREVIEW_LIMIT:
        result["notice"] = "64MB를 넘는 파일은 정보만 보여요."
        return result
    structured = key.endswith((".metadata.json", ".avro", ".parquet"))
    # IfMatch prevents a replacement between HEAD and GET from exceeding the cap.
    data = b""
    if size:
        response = s3.get_object(Bucket=bucket, Key=key, IfMatch=head["ETag"],
                                 **({} if structured else {"Range": "bytes=0-4095"}))
        with closing(response["Body"]) as body:
            data = body.read(PREVIEW_LIMIT + 1 if structured else 4096)
    if len(data) > PREVIEW_LIMIT:
        raise ValueError("파일 크기가 미리보기 제한을 넘었습니다.")
    if key.endswith(".metadata.json"):
        document = json.loads(data)
        result.update(kind="json", summary={k: document.get(k) for k in (
            "current-snapshot-id", "snapshots", "schemas", "partition-specs", "properties")},
                      document=document)
    elif key.endswith(".avro"):
        import fastavro
        with io.BytesIO(data) as stream:
            records = list(itertools.islice(fastavro.reader(stream), 201))
        result.update(kind="avro", records=records[:200], truncated=len(records) > 200)
    elif key.endswith(".parquet"):
        import pyarrow.parquet as pq
        with io.BytesIO(data) as stream:
            parquet = pq.ParquetFile(stream)
            groups = []
            for index in range(parquet.metadata.num_row_groups):
                group = parquet.metadata.row_group(index)
                columns = []
                for col_index in range(group.num_columns):
                    column = group.column(col_index)
                    stats = column.statistics
                    columns.append({"column": column.path_in_schema,
                                    "min": stats.min if stats and stats.has_min_max else None,
                                    "max": stats.max if stats and stats.has_min_max else None,
                                    "null_count": stats.null_count if stats else None})
                groups.append({"row_group": index, "rows": group.num_rows, "columns": columns})
            first = next(parquet.iter_batches(batch_size=50), None)
            result.update(kind="parquet", schema=str(parquet.schema_arrow),
                          total_rows=parquet.metadata.num_rows, row_groups=groups,
                          records=first.to_pylist() if first is not None else [])
    else:
        try:
            text = data.decode("utf-8")
            if any(ord(c) < 32 and c not in "\n\r\t" for c in text):
                raise UnicodeError()
            result.update(kind="text", text=text)
        except UnicodeError:
            result.update(kind="hex", text=data.hex(" "))
    return result
