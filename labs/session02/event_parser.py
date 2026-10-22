"""Read-only teaching parser; deliberately separate from the running Flink job."""
import argparse
import json
from pathlib import Path


def parse_event(raw_key, raw_value, metadata):
    """Interpret one schemaless Debezium record without changing its source metadata.

    Kafka wire null (None) is a tombstone. JSON text 'null' is not a wire null.
    This teaching parser supports row events r/c/u/d, not all Debezium messages.
    """
    result = {'kind': 'invalid', 'metadata': dict(metadata)}

    def invalid(code):
        return {**result, 'error_code': code}

    def decode(raw):
        if not isinstance(raw, (str, bytes, bytearray)):
            raise TypeError('Expected wire bytes, str or None')
        return json.loads(raw)

    try:
        key = decode(raw_key)
    except (ValueError, TypeError, UnicodeError):
        return invalid('invalid_key_json')
    if not isinstance(key, dict) or not key or any(v is None or isinstance(v, (dict, list, bool)) for v in key.values()):
        return invalid('invalid_key_type')
    if raw_value is None:
        return {**result, 'kind': 'tombstone', 'op': None, 'pk': key, 'value_kind': 'wire_null'}
    try:
        value = decode(raw_value)
    except (ValueError, TypeError, UnicodeError):
        return invalid('invalid_value_json')
    if value is None:
        return invalid('invalid_json_null')
    if not isinstance(value, dict):
        return invalid('invalid_envelope_type')
    op = value.get('op')
    if op not in ('r', 'c', 'u', 'd'):
        return invalid('unsupported_op')
    before, after = value.get('before'), value.get('after')
    if any(row is not None and not isinstance(row, dict) for row in (before, after)):
        return invalid('invalid_row_type')
    pk_source = 'before' if op == 'd' else 'after'
    row = before if op == 'd' else after
    if not isinstance(row, dict) or any(k not in row or row[k] != v for k, v in key.items()):
        return invalid('missing_or_mismatched_row_key')
    source = value.get('source')
    if source is not None and not isinstance(source, dict):
        return invalid('invalid_source_type')
    return {**result, 'kind': 'change', 'op': op, 'pk': {k: row[k] for k in key},
            'pk_source': pk_source, 'before': before, 'after': after, 'row': row,
            'source_lsn': (source or {}).get('lsn')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixtures', action='store_true', required=True)
    args = parser.parse_args()
    for line in Path(__file__).with_name('fixtures').joinpath('events.jsonl').read_text().splitlines():
        sample = json.loads(line)
        result = parse_event(sample['key'], sample['value'], sample['metadata'])
        print(json.dumps({'origin': 'synthetic_fixture', 'sample_id': sample['sample_id'], **result}, ensure_ascii=False))


if __name__ == '__main__':
    main()
