import json
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import quote_plus

os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

import zstandard

import zst_to_json


def s3_event(key, size=None):
    object_info = {"key": quote_plus(key)}
    if size is not None:
        object_info["size"] = size
    return {
        "Records": [
            {"s3": {"bucket": {"name": "source-bucket"}, "object": object_info}}
        ]
    }


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.context = SimpleNamespace(aws_request_id="request-123")
        self.s3 = Mock()
        self.uploaded = {}
        self.download_path = None

        def download(bucket, key, path):
            self.download_path = path
            Path(path).write_bytes(self.source_data)

        def upload(path, bucket, key, ExtraArgs):
            self.uploaded[key] = (json.loads(Path(path).read_text()), ExtraArgs)

        self.s3.download_file.side_effect = download
        self.s3.upload_file.side_effect = upload
        self.patcher = patch.object(zst_to_json, "s3_client", self.s3)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_compressed_journal_extracts_outer_object_without_logging_data(self):
        payload = {
            "eventName": "Create{Thing}",
            "eventSource": "aws.db",
            "detail": {"nested": {"secret": "PRIVATE_SENTINEL"}},
        }
        journal = b"\x00not-json {broken}\n" + json.dumps(payload).encode() + b"\x00tail"
        self.source_data = zstandard.ZstdCompressor().compress(journal)

        with self.assertLogs(zst_to_json.logger, level="INFO") as captured:
            result = zst_to_json.lambda_handler(
                s3_event("logs/db_123-456/journal.zst", len(self.source_data)),
                self.context,
            )

        self.assertEqual(result["statusCode"], 200)
        output, headers = self.uploaded[
            "output/Create_Thing_aws_db_db_123-456.json"
        ]
        self.assertEqual(output, payload)
        self.assertEqual(headers, {"ContentType": "application/json"})
        self.assertNotIn("PRIVATE_SENTINEL", "\n".join(captured.output))
        self.assertFalse(Path(self.download_path).exists())

    def test_raw_fallback_and_missing_size(self):
        self.source_data = b"binary\x00" + json.dumps(
            {"eventName": "Delete", "eventSource": "service.example"}
        ).encode()

        result = zst_to_json.lambda_handler(
            s3_event("other/journal.zst"), self.context
        )

        self.assertEqual(result["statusCode"], 200)
        self.assertIn(
            "output/Delete_service_example_unknown_request-123.json",
            self.uploaded,
        )

    def test_temp_files_are_removed_when_upload_fails(self):
        self.source_data = json.dumps({"eventName": "Create"}).encode()
        self.s3.upload_file.side_effect = RuntimeError("upload failed")

        with self.assertRaisesRegex(RuntimeError, "upload failed"):
            zst_to_json.lambda_handler(
                s3_event("other/journal.zst", len(self.source_data)),
                self.context,
            )

        self.assertFalse(Path(self.download_path).exists())

    def test_metadata_cannot_escape_output_prefix(self):
        self.source_data = json.dumps(
            {"eventName": "../private/path", "eventSource": "a/b"}
        ).encode()

        with patch.dict(os.environ, {"OUTPUT_PREFIX": "cleaned"}):
            zst_to_json.lambda_handler(
                s3_event("other/journal.zst", len(self.source_data)),
                self.context,
            )

        self.assertEqual(
            list(self.uploaded),
            ["cleaned/private_path_a_b_unknown_request-123.json"],
        )


if __name__ == "__main__":
    unittest.main()
