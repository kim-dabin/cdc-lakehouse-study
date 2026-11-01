import datetime
import io
import unittest
from unittest.mock import MagicMock, patch

from tools.explorer.app import storage


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MagicMock()
        self.s3.head_object.return_value = {
            "ContentLength": 10, "LastModified": datetime.datetime(2026, 1, 1), "ETag": '"abc"'}
        self.client_patch = patch.object(storage, "client", return_value=self.s3)
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)

    def test_large_object_is_head_only(self):
        self.s3.head_object.return_value["ContentLength"] = storage.PREVIEW_LIMIT + 1
        result = storage.preview("s3://warehouse/large.parquet")
        self.assertEqual(result["kind"], "metadata")
        self.s3.get_object.assert_not_called()

    def test_unknown_file_reads_only_first_4kb_and_closes(self):
        body = io.BytesIO(b"hello")
        self.s3.get_object.return_value = {"Body": body}
        result = storage.preview("s3a://warehouse/file.txt")
        self.assertEqual(result["text"], "hello")
        self.assertTrue(body.closed)
        self.s3.get_object.assert_called_once_with(Bucket="warehouse", Key="file.txt",
                                                   IfMatch='"abc"', Range="bytes=0-4095")

    def test_empty_file_has_empty_preview(self):
        self.s3.head_object.return_value["ContentLength"] = 0
        result = storage.preview("s3://warehouse/empty.txt")
        self.assertEqual(result["text"], "")
        self.s3.get_object.assert_not_called()

    def test_binary_and_metadata_json(self):
        for key, data, kind in (("a.bin", b"\x00\xff", "hex"),
                                ("a.metadata.json", b'{"current-snapshot-id":1}', "json")):
            self.s3.get_object.return_value = {"Body": io.BytesIO(data)}
            result = storage.preview(f"s3://warehouse/{key}")
            self.assertEqual(result["kind"], kind)
            if kind == "json":
                self.assertEqual(result["summary"]["current-snapshot-id"], 1)

    def test_other_bucket_never_touches_s3(self):
        with self.assertRaises(ValueError):
            storage.preview("s3://other/key")
        self.s3.head_object.assert_not_called()
        self.s3.get_object.assert_not_called()

    def test_pagination_and_read_only_calls(self):
        self.s3.list_objects_v2.return_value = {
            "CommonPrefixes": [{"Prefix": "silver/"}], "Contents": [], "NextContinuationToken": "next"}
        result = storage.list_files("", "previous")
        self.assertEqual(result["next_token"], "next")
        self.assertEqual(result["folders"], ["silver/"])
        self.s3.list_objects_v2.assert_called_once_with(
            Bucket="warehouse", Prefix="", Delimiter="/", MaxKeys=1000, ContinuationToken="previous")
