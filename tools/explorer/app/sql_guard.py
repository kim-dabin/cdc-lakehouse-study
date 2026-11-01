"""Conservative read-only Spark SQL gate; deliberately has no dependencies."""
import re

MAX_SQL_LENGTH = 20_000
ALLOWED = {"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "VALUES", "TABLE"}
FORBIDDEN = set("""INSERT UPDATE DELETE MERGE CREATE DROP ALTER TRUNCATE CALL REFRESH
CACHE UNCACHE CLEAR SET RESET USE ADD LOAD MSCK REPAIR ANALYZE GRANT REVOKE
OPTIMIZE VACUUM COPY EXPORT IMPORT LOCK UNLOCK EXECUTE""".split())


def _mask(sql: str) -> str:
    """Mask comments and quoted content without joining adjacent SQL tokens."""
    out = []
    i = 0
    while i < len(sql):
        if sql.startswith("--", i):
            end = i + 2
            while end < len(sql) and sql[end] not in "\r\n":
                end += 1
            out.append(" ")
            i = end
        elif sql.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(sql) and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise ValueError("주석이 닫히지 않았습니다.")
            out.append(" ")
        elif sql[i] in "'\"`":
            quote = sql[i]
            i += 1
            while i < len(sql):
                if sql[i] == "\\" and quote != "`":
                    i += 2
                elif sql[i] == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    i += 1
            else:
                raise ValueError("따옴표가 닫히지 않았습니다.")
            out.append(" ? ")
        else:
            out.append(sql[i])
            i += 1
    return "".join(out)


def validate_sql(sql: str) -> None:
    """Raise ValueError before opening a connection for any rejected input."""
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL을 입력해 주세요.")
    if len(sql) > MAX_SQL_LENGTH:
        raise ValueError("SQL은 20,000자까지 입력할 수 있습니다.")
    masked = _mask(sql).strip()
    if masked.endswith(";"):
        masked = masked[:-1].rstrip()
    if ";" in masked:
        raise ValueError("한 번에 한 문장만 실행할 수 있습니다.")
    first = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\b", masked)
    if not first or first[1].upper() not in ALLOWED:
        raise ValueError("조회 문장만 실행할 수 있습니다.")
    show_create = re.match(r"SHOW\s+CREATE\s+TABLE\b", masked, re.IGNORECASE)
    for word in re.finditer(r"\b[A-Za-z_][A-Za-z0-9_]*\b", masked):
        if word[0].upper() in FORBIDDEN:
            if word[0].upper() == "CREATE" and show_create and word.start() < show_create.end():
                continue
            raise ValueError(f"조회 전용입니다. {word[0].upper()} 문장은 실행할 수 없습니다.")
    # SELECT 안에서도 JVM 정적 메서드 호출(reflect 계열)과 외부 스크립트 실행(TRANSFORM ... USING '...')이 가능하다.
    # 공유 Spark Thrift 서버를 멈추거나 명령을 실행할 수 있으므로 함께 막는다.
    func = re.search(r"\b(reflect|java_method|try_reflect)\s*\(", masked, re.IGNORECASE)
    if func:
        raise ValueError(f"조회 전용입니다. {func[1].lower()} 함수는 실행할 수 없습니다.")
    if re.search(r"\bUSING\s+\?", masked, re.IGNORECASE):
        raise ValueError("조회 전용입니다. 외부 스크립트를 실행하는 TRANSFORM ... USING은 쓸 수 없습니다.")
