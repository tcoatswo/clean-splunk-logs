"""Extract a JSON event from a Splunk journal and save it to S3."""

import json
import logging
import os
import re
import tempfile
from io import BytesIO
from urllib.parse import unquote_plus

import boto3
import zstandard


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3_client = boto3.client("s3")


def decompress_zst_stream(compressed_data):
    """Return decompressed UTF-8 text, or None for non-Zstandard input."""
    try:
        output = BytesIO()
        zstandard.ZstdDecompressor().copy_stream(BytesIO(compressed_data), output)
        return output.getvalue().decode("utf-8", errors="replace")
    except zstandard.ZstdError:
        logger.info(
            "Input is not a readable Zstandard stream; trying raw JSON extraction"
        )
        return None


def clean_and_extract_json(raw_data):
    """Return the first valid JSON object surrounded by journal noise."""
    raw_text = (
        raw_data.decode("utf-8", errors="replace")
        if isinstance(raw_data, bytes)
        else raw_data
    )
    cleaned_text = "".join(
        char for char in raw_text if ord(char) >= 32 or char in "\n\r\t"
    )

    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", cleaned_text):
        try:
            value, end = decoder.raw_decode(cleaned_text, match.start())
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return cleaned_text[match.start() : end]

    logger.error("No valid JSON object found in journal")
    return None


def get_unique_identifier(key, request_id):
    """Extract an identifier from the S3 key, falling back to the request ID."""
    for part in key.split("/"):
        if part.startswith("db_") and "-" in part:
            return part
    return f"unknown_{request_id}"


def safe_filename_part(value):
    """Keep event metadata from introducing paths into the output key."""
    if not isinstance(value, str):
        return "unknown"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "unknown"


def lambda_handler(event, context):
    """Process journal.zst objects in an S3 ObjectCreated event."""
    if not event.get("Records"):
        logger.warning("No records found in event")
        return {"statusCode": 400, "body": json.dumps("No records in event")}

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        object_info = record["s3"]["object"]
        key = unquote_plus(object_info["key"])

        if not key.endswith("journal.zst"):
            logger.info("Skipping non-journal object: %s", key)
            continue

        # A separate directory per record prevents failed invocations from leaving
        # files behind and avoids using untrusted metadata as a local file path.
        with tempfile.TemporaryDirectory(prefix="splunk-journal-") as temp_dir:
            temp_zst = os.path.join(temp_dir, "journal.zst")
            temp_json = os.path.join(temp_dir, "event.json")
            s3_client.download_file(bucket, key, temp_zst)

            file_size = os.path.getsize(temp_zst)
            if file_size == 0:
                raise ValueError(f"Empty file: {key}")
            expected_size = object_info.get("size")
            if expected_size is not None and file_size != expected_size:
                raise ValueError(f"Size mismatch for {key}")

            with open(temp_zst, "rb") as source:
                raw_data = source.read()
            decompressed = decompress_zst_stream(raw_data)
            content = clean_and_extract_json(
                raw_data if decompressed is None else decompressed
            )
            if content is None:
                raise ValueError(f"Unable to extract JSON from {key}")

            json_data = json.loads(content)
            unique_id = get_unique_identifier(key, context.aws_request_id)
            event_name = safe_filename_part(json_data.get("eventName", "unknown"))
            event_source = json_data.get("eventSource", "unknown")
            if isinstance(event_source, str):
                event_source = event_source.replace(".", "_")
            event_source = safe_filename_part(event_source)
            output_filename = (
                f"{event_name}_{event_source}_{safe_filename_part(unique_id)}.json"
            )
            prefix = os.environ.get("OUTPUT_PREFIX", "output/")
            output_key = (
                f"{prefix.rstrip('/')}/{output_filename}"
                if prefix
                else output_filename
            )

            with open(temp_json, "w", encoding="utf-8") as output:
                json.dump(json_data, output, indent=2)

            s3_client.upload_file(
                temp_json,
                bucket,
                output_key,
                ExtraArgs={"ContentType": "application/json"},
            )
            logger.info("Processed %s to s3://%s/%s", key, bucket, output_key)

    return {
        "statusCode": 200,
        "body": json.dumps("Successfully processed S3 event"),
    }
