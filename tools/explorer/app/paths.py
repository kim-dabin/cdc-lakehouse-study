"""S3 URI parsing shared by the API and dependency-free path tests."""
import re
from urllib.parse import urlsplit


def parse_path(path: str, bucket: str = "warehouse") -> tuple[str, str]:
    if not isinstance(path, str) or any(ord(c) < 32 for c in path):
        raise ValueError("올바른 S3 경로를 입력해 주세요.")
    parsed = urlsplit(path)
    if (parsed.scheme not in {"s3", "s3a"} or parsed.netloc != bucket
            or not path.startswith((f"s3://{bucket}/", f"s3a://{bucket}/"))
            or "?" in path or "#" in path or not parsed.path[1:]):
        raise ValueError(f"s3://{bucket}/ 또는 s3a://{bucket}/ 경로만 열 수 있습니다.")
    return bucket, parsed.path[1:]


def table_identifier(namespace: str, table: str | None = None) -> str:
    parts = [namespace] if table is None else [namespace, table]
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError("이름에는 영문, 숫자, 밑줄만 사용할 수 있습니다.")
    return "lake." + ".".join(parts)
