"""Small OpenMetadata client for the lab (runs in the runner container).

  token      print the ingestion-bot JWT (used by scripts/governance.sh, never committed)
  contracts  create or update one OpenMetadata data contract per dbt model with an enforced
             contract (columns and types from the dbt YAML plus one semantics rule), then run the
             contract validation. dbt test results are imported by the dbt ingestion and show on
             the table's Data Quality tab; they are not linked as contract quality expectations
             because OpenMetadata runs those through its pipeline service, which this lab does not run.
  lineage    print upstream tables and column lineage that dbt ingestion created

Lab defaults only: local admin login (admin@open-metadata.org / admin) on the om-server container.
"""
import argparse
import base64
import json
import os
import sys
import time
from urllib.parse import quote

import requests

BASE = os.environ.get("OM_URL", "http://om-server:8585/api")
SERVICE = os.environ.get("OM_SERVICE", "cdc_lakehouse")
DATABASE = os.environ.get("OM_DATABASE", "default")  # Hive-type services keep one database named default
MANIFEST = os.environ.get("DBT_MANIFEST", "dbt/target/manifest.json")
TYPES = {"string": "STRING", "bigint": "BIGINT", "int": "INT", "date": "DATE",
         "timestamp": "TIMESTAMP", "boolean": "BOOLEAN", "double": "DOUBLE"}


def wait_for_server():
    for _ in range(120):
        try:
            if requests.get(f"{BASE}/v1/system/version", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    sys.exit("OpenMetadata is not reachable at " + BASE)


def admin_session():
    wait_for_server()
    r = requests.post(f"{BASE}/v1/users/login", timeout=30, json={
        "email": "admin@open-metadata.org", "password": base64.b64encode(b"admin").decode()})
    r.raise_for_status()
    s = requests.Session()
    s.headers.update({"Authorization": "Bearer " + r.json()["accessToken"], "Content-Type": "application/json"})
    return s


def call(s, method, path, **kw):
    r = s.request(method, BASE + path, timeout=60, **kw)
    if not r.ok:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:400]}")
    return r.json() if r.text else {}


def token():
    s = admin_session()
    bot = call(s, "GET", "/v1/bots/name/ingestion-bot")
    mech = call(s, "GET", f"/v1/users/auth-mechanism/{bot['botUser']['id']}")
    print(mech["config"]["JWTToken"])


def contract_models():
    manifest = json.load(open(MANIFEST))
    for node in manifest["nodes"].values():
        if node["resource_type"] == "model" and node["config"].get("contract", {}).get("enforced"):
            yield node


def table_fqn(node):
    return f"{SERVICE}.{DATABASE}.{node['schema']}.{node['name']}"


def contracts():
    s = admin_session()
    failed = 0
    for node in contract_models():
        fqn = table_fqn(node)
        table = call(s, "GET", f"/v1/tables/name/{quote(fqn, safe='')}", params={"fields": "columns"})
        link = f"<#E::table::{fqn}>"
        tests = call(s, "GET", "/v1/dataQuality/testCases",
                     params={"entityLink": link, "includeAllTests": "true", "limit": 100}).get("data", [])
        columns = [{"name": c["name"], "dataType": TYPES.get((c.get("data_type") or "").lower(), "STRING"),
                    "description": c.get("description") or ""}
                   for c in node["columns"].values()]
        body = {
            "name": f"{node['schema']}.{node['name']} (dbt contract)",
            "entity": {"id": table["id"], "type": "table"},
            "description": (f"dbt model contract (contract.enforced: true) for {node['schema']}.{node['name']}. "
                            "dbt checks these column names and types before it builds the table; "
                            f"{len(tests)} dbt tests on this table check the data (Data Quality tab). "
                            + (node.get("description") or "")),
            "entityStatus": "Approved",
            "schema": columns,
            "semantics": [{"name": "Table is described",
                           "description": "The table must carry a description from dbt.",
                           "rule": json.dumps({"!!": {"var": "description"}})}],
        }
        contract = call(s, "PUT", "/v1/dataContracts", json=body)
        result = call(s, "POST", f"/v1/dataContracts/{contract['id']}/validate")
        status = result.get("contractExecutionStatus") or result.get("status") or result
        print(json.dumps({"table": fqn, "contract": contract["fullyQualifiedName"], "columns": len(columns),
                          "dbt_tests_on_table": len(tests), "validation": status,
                          "schema": result.get("schemaValidation"), "semantics": result.get("semanticsValidation")}, ensure_ascii=False))
        failed += str(status).lower() in ("failed", "aborted")
    return 1 if failed else 0


def lineage():
    s = admin_session()
    for node in contract_models():
        fqn = table_fqn(node)
        data = call(s, "GET", f"/v1/lineage/table/name/{quote(fqn, safe='')}",
                    params={"upstreamDepth": 1, "downstreamDepth": 0})
        names = {e["id"]: e["fullyQualifiedName"] for e in data.get("nodes", [])}
        names[data["entity"]["id"]] = data["entity"]["fullyQualifiedName"]
        for edge in data.get("upstreamEdges", []):
            details = edge.get("lineageDetails") or {}
            cols = [(", ".join(c.split(".")[-1] for c in (cl.get("fromColumns") or [])), cl.get("toColumn", "").split(".")[-1])
                    for cl in details.get("columnsLineage") or []]
            print(json.dumps({"to": fqn.split(".", 2)[-1], "from": names.get(edge["fromEntity"], edge["fromEntity"]).split(".", 2)[-1],
                              "source": details.get("source"), "column_lineage": cols}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["token", "contracts", "lineage"])
    cmd = parser.parse_args().command
    sys.exit({"token": token, "contracts": contracts, "lineage": lineage}[cmd]() or 0)
