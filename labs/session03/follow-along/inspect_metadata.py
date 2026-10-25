"""Read a downloaded Iceberg metadata JSON; never contacts or changes a service."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("metadata_file", type=Path)
args = parser.parse_args()
metadata = json.loads(args.metadata_file.read_text(encoding="utf-8"))
current = metadata["current-snapshot-id"]
matches = [s for s in metadata["snapshots"] if s["snapshot-id"] == current]
if len(matches) != 1:
    raise SystemExit("Expected exactly one snapshot matching current-snapshot-id")
snapshot = matches[0]
print(json.dumps({
    "downloaded_file": args.metadata_file.name,
    "current-snapshot-id": current,
    "matching-snapshot-id": snapshot["snapshot-id"],
    "operation": snapshot["summary"]["operation"],
    "manifest-list": snapshot["manifest-list"],
}, ensure_ascii=False, indent=2))
