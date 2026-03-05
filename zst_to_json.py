import json
import os
import boto3
import zstandard
import re
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError
import logging
from io import BytesIO

# Initialize logger globally so it persists across Lambda invocations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client outside the handler so it can be reused across warm starts
s3_client = boto3.client('s3')

def decompress_zst_stream(compressed_data):
    """Attempt to decompress zstd data using streaming."""
    try:
        dctx = zstandard.ZstdDecompressor()
        # Use BytesIO to treat the raw bytes in memory like file streams
        input_stream = BytesIO(compressed_data)
        output_stream = BytesIO()
        
        # copy_stream is memory efficient; it streams chunks rather than loading it all at once
        dctx.copy_stream(input_stream, output_stream)
        
        # Decode the raw bytes into a UTF-8 string, ignoring errors to prevent crashes on bad characters
        decompressed = output_stream.getvalue().decode('utf-8', errors='ignore')
        logger.info(f"Decompressed content (first 1000 chars): {decompressed[:1000]}")
        return decompressed
    except zstandard.ZstdError as e:
        logger.error(f"Zstandard streaming decompression failed: {str(e)}")
        return None

def clean_and_extract_json(raw_data):
    """Clean binary bytes and extract JSON from raw or decompressed data."""
    try:
        # Step 1: Ensure we are working with text, replacing unreadable binary characters
        raw_text = raw_data.decode('utf-8', errors='replace') if isinstance(raw_data, bytes) else raw_data
        logger.info(f"Raw decoded text (first 1000 chars): {raw_text[:1000]}")

        # Step 2: Strip out control characters (like ASCII bells/whistles), but keep newlines/tabs
        cleaned_text = ''.join(char for char in raw_text if ord(char) >= 32 or char in '\n\r\t')
        logger.info(f"Cleaned text (first 1000 chars): {cleaned_text[:1000]}")

        # Step 3: Try regex to find a JSON object block. 
        # This regex looks for { ... } and can handle one level of nested braces inside.
        json_match = re.search(r'\{(?:[^{}]|\{[^{}]*\})*\}', cleaned_text, re.DOTALL)
        if json_match:
            json_content = json_match.group()
            logger.info(f"Extracted JSON via regex (first 1000 chars): {json_content[:1000]}")
            return json_content

        # Step 4: Fallback method if regex fails. Parse line-by-line looking for { and } boundaries.
        json_objects = []
        current_json = []
        for line in cleaned_text.splitlines():
            line = line.strip()
            if line.startswith('{'): # Start of a new JSON object
                current_json.append(line)
            elif current_json and line.endswith('}'): # End of the JSON object
                current_json.append(line)
                json_objects.append('\n'.join(current_json))
                current_json = [] # Reset for the next potential object
            elif current_json: # Middle of a JSON object
                current_json.append(line)
                
        # If the fallback found valid objects, return the first one
        if json_objects:
            json_content = json_objects[0] 
            logger.info(f"Extracted JSON via line-by-line (first 1000 chars): {json_content[:1000]}")
            logger.info(f"Found {len(json_objects)} JSON objects")
            return json_content

        # If both extraction methods fail, return None
        logger.error("No valid JSON found in cleaned data")
        return None
    except Exception as e:
        logger.error(f"Failed to clean and extract JSON: {str(e)}")
        return None

def get_unique_identifier(key, request_id):
    """Extract a unique identifier from the S3 key."""
    parts = key.split('/')
    # Look through the folders/path of the S3 key
    for part in parts:
        # Assumes the identifier folder looks like "db_something-something"
        if part.startswith('db_') and '-' in part:
            return part
    
    # Fallback to the Lambda request ID if the expected folder structure isn't there
    return f"unknown_{request_id}"

def lambda_handler(event, context):
    """Lambda handler for processing S3 PutObject events for Splunk journal.zst files."""
    try:
        # Validate that the event actually contains S3 records
        if not event.get('Records'):
            logger.warning("No records found in event")
            return {'statusCode': 400, 'body': json.dumps('No records in event')}

        logger.info(f"Zstandard version: {zstandard.__version__}")

        # Loop through all files that triggered this Lambda (usually just 1, but can be multiple)
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            
            # unquote_plus handles spaces and special characters in the S3 filename (e.g., converts '+' to ' ')
            key = unquote_plus(record['s3']['object']['key'])
            s3_size = record['s3']['object'].get('size', 0)
            
            logger.info(f"Received event with bucket: {bucket}, key: {key}")
            logger.info(f"Processing {key}, S3 size: {s3_size} bytes")
            key_parts = key.split('/')
            logger.info(f"Key parts: {key_parts}")

            # Ignore any files that aren't the specific target file
            if not key.endswith('journal.zst'):
                logger.info(f"Skipping {key}: not a journal.zst file")
                continue

            # Lambda environments allow writing to the /tmp/ directory (up to 512MB by default)
            temp_zst = f"/tmp/journal.zst"
            
            try:
                # Download the compressed file from S3 to the Lambda's local /tmp/ storage
                s3_client.download_file(bucket, key, temp_zst)
                file_size = os.path.getsize(temp_zst)
                logger.info(f"Downloaded {key} to {temp_zst}, size: {file_size} bytes")
                
                # Sanity checks to ensure the file downloaded correctly
                if file_size != s3_size:
                    logger.error(f"Downloaded size mismatch: S3 size={s3_size}, local size={file_size}")
                    raise ValueError(f"Size mismatch for {key}")
                if file_size == 0:
                    logger.error(f"Downloaded file {key} is empty")
                    raise ValueError(f"Empty file: {key}")
            except ClientError as e:
                logger.error(f"Failed to download {key}: {str(e)}")
                raise

            # Load the raw bytes of the downloaded file into memory
            with open(temp_zst, 'rb') as f:
                raw_data = f.read()
            if not raw_data:
                logger.error(f"Empty file content from {key}")
                raise ValueError(f"Empty file content: {key}")

            # Step 1 of Data Processing: Try decompressing the Zstandard file
            content = decompress_zst_stream(raw_data)
            
            if content is None:
                # If decompression failed, the file might not actually be compressed.
                # Try to extract JSON directly from the raw data.
                logger.info(f"First 10 bytes of {key}: {raw_data[:10].hex()}")
                content = clean_and_extract_json(raw_data)
            else:
                # If decompression succeeded, clean the decompressed text and grab the JSON payload
                content = clean_and_extract_json(content)

            # Abort if we couldn't find any JSON in the file
            if content is None:
                logger.error(f"Failed to extract JSON from {key}")
                raise ValueError(f"Unable to extract JSON from {key}")

            # Parse the extracted text string into an actual Python dictionary
            try:
                json_data = json.loads(content)
                logger.info(f"Successfully parsed JSON from {key}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {key}: {str(e)}")
                logger.info(f"Content attempted (first 1000 chars): {content[:1000]}")
                raise ValueError(f"Invalid JSON in {key}")

            # Create a smart filename for the output file based on the data inside it
            unique_id = get_unique_identifier(key, context.aws_request_id)
            logger.info(f"Extracted unique identifier: {unique_id}")
            
            # Extract routing keys from the JSON (defaults to 'unknown' if not present)
            event_name = json_data.get('eventName', 'unknown')
            event_source = json_data.get('eventSource', 'unknown').replace('.', '_')
            
            # Construct the final filename and S3 path (e.g., output/MyEvent_aws_db_123.json)
            output_filename = f"{event_name}_{event_source}_{unique_id}.json"
            output_key = f"{os.environ.get('OUTPUT_PREFIX', 'output/')}{output_filename}"

            # Save the nicely formatted JSON back to the /tmp/ directory
            temp_json = f"/tmp/{output_filename}"
            with open(temp_json, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2)

            # Upload the finished JSON file back to the same S3 bucket
            try:
                s3_client.upload_file(temp_json, bucket, output_key)
                logger.info(f"Processed {key}. Saved JSON to s3://{bucket}/{output_key}")
            except ClientError as e:
                logger.error(f"Failed to upload {output_key}: {str(e)}")
                raise

            # Housekeeping: Delete the temporary files from Lambda's /tmp/ directory
            # This prevents the Lambda from running out of disk space on subsequent warm invocations
            for temp_file in [temp_zst, temp_json]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

        # Standard Lambda success response
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed S3 event')
        }

    except Exception as e:
        # Catch any unexpected errors, log them, and bubble the error up so Lambda registers a failure
        logger.error(f"Error processing event: {str(e)}")
        raise
