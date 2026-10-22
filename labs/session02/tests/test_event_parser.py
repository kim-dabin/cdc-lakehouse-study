"""Behavior checks for wire semantics and DELETE key preservation, no services."""
import copy
import json
import unittest
from labs.session02.event_parser import parse_event


class ParserTests(unittest.TestCase):
    key = b'{"refund_id":"fixture-r1"}'
    metadata = {'topic': 'fixture.refunds', 'partition_id': 0, 'kafka_offset': 5}

    def parse(self, value, key=None):
        return parse_event(self.key if key is None else key, value, self.metadata)

    def envelope(self, op):
        before = {'refund_id': 'fixture-r1', 'amount': 20000} if op in ('u', 'd') else None
        after = {'refund_id': 'fixture-r1', 'amount': 30000} if op != 'd' else None
        return json.dumps({'op': op, 'before': before, 'after': after, 'source': {'lsn': 123}})

    def test_changes_keep_identity_and_delete_uses_before(self):
        for op in 'rcud':
            with self.subTest(op=op):
                event = self.parse(self.envelope(op))
                self.assertEqual(event['kind'], 'change')
                self.assertEqual(event['op'], op)
                self.assertEqual(event['pk'], {'refund_id': 'fixture-r1'})
                self.assertEqual(event['pk_source'], 'before' if op == 'd' else 'after')
                self.assertEqual(event['row']['amount'], 20000 if op == 'd' else 30000)
                self.assertEqual(event['source_lsn'], 123)

    def test_wire_null_is_not_json_null_or_empty(self):
        self.assertEqual(self.parse(None)['kind'], 'tombstone')
        self.assertEqual(self.parse('null')['error_code'], 'invalid_json_null')
        self.assertEqual(self.parse(b'')['error_code'], 'invalid_value_json')

    def test_invalid_json_is_quarantined_and_next_event_survives(self):
        self.assertEqual(self.parse('{broken')['error_code'], 'invalid_value_json')
        self.assertEqual(self.parse(self.envelope('c'))['kind'], 'change')

    def test_invalid_key_shapes_and_missing_pk(self):
        for key in ('[]', 'null', '42', '{}', '{"refund_id":null}', '{"refund_id":[]}'):
            with self.subTest(key=key):
                self.assertEqual(self.parse(self.envelope('c'), key)['error_code'], 'invalid_key_type')
        self.assertEqual(self.parse('{"op":"d","before":{},"after":null}')['error_code'], 'missing_or_mismatched_row_key')

    def test_mismatched_key_cannot_silently_pass(self):
        self.assertEqual(self.parse(self.envelope('d'), '{"refund_id":"other"}')['error_code'], 'missing_or_mismatched_row_key')

    def test_unsupported_envelope_and_bad_source(self):
        self.assertEqual(self.parse('[]')['error_code'], 'invalid_envelope_type')
        self.assertEqual(self.parse('{"op":"t"}')['error_code'], 'unsupported_op')
        self.assertEqual(self.parse('{"op":"c","after":[],"before":null}')['error_code'], 'invalid_row_type')
        self.assertEqual(self.parse('{"op":"c","after":{"refund_id":"fixture-r1"},"source":[]}')['error_code'], 'invalid_source_type')

    def test_non_utf8_is_invalid_not_a_tombstone(self):
        self.assertEqual(self.parse(b'\xff')['error_code'], 'invalid_value_json')
        self.assertEqual(self.parse(self.envelope('c'), b'\xff')['error_code'], 'invalid_key_json')

    def test_metadata_is_preserved_without_mutating_input(self):
        original = copy.deepcopy(self.metadata)
        output = self.parse(self.envelope('c'))
        output['metadata']['kafka_offset'] = 999
        self.assertEqual(self.metadata, original)


if __name__ == '__main__':
    unittest.main()
