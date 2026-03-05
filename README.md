# Splunk Journal ZST Processor (AWS Lambda)

This AWS Lambda function automatically processes compressed Splunk journal files (`journal.zst`) uploaded to an Amazon S3 bucket. It decompresses the files using Zstandard, extracts and validates the underlying JSON data, and saves the formatted JSON back to the S3 bucket. This is specifically designed to strip proprietary binary data from .zst files, yielding clean JSON output.

## Features

* **Automated Triggering:** Runs automatically via S3 PutObject events.
* **Streaming Decompression:** Uses memory-efficient streaming to decompress `.zst` files without overloading Lambda memory limits.
* **Robust Data Extraction:** Includes fallback mechanisms (Regex and line-by-line parsing) to scrub binary artifacts and extract valid JSON payloads.
* **Smart Routing:** Parses the internal JSON to generate dynamic, descriptive output filenames based on `eventName` and `eventSource`.
* **Self-Cleaning:** Cleans up temporary files in `/tmp/` to prevent storage exhaustion on warm Lambda starts.

## Prerequisites

* AWS Account with permissions to create Lambda functions, IAM roles, and S3 buckets.
* Python 3.8+ runtime environment.
* The `zstandard` Python package (must be bundled with your deployment package or added as a Lambda Layer).

## Environment Variables

You can configure the behavior of the Lambda function using the following environment variable:

| Variable | Description | Default Value |
| :--- | :--- | :--- |
| `OUTPUT_PREFIX` | The S3 folder/prefix where the processed JSON files will be saved. | `output/` |

## Deployment Instructions

1. **Package the Code:** Because this script relies on the external `zstandard` library, you must package the dependency with your code. 
   ```bash
   pip install zstandard -t .
   zip -r function.zip .

1. **Create the Lambda Function:** Upload the `function.zip` to AWS Lambda and set the runtime to Python (e.g., Python 3.12). Set the handler to `lambda_function.lambda_handler` (assuming your file is named `lambda_function.py`).
2. **Configure S3 Trigger:** Go to your S3 bucket and configure an Event Notification to trigger this Lambda function on `s3:ObjectCreated:*` events. 
   > **Tip:** Add a suffix filter for `.zst` to prevent the Lambda from triggering unnecessarily.
3. **Adjust Timeout and Memory:** Decompressing and processing large files can take time. Increase the Lambda timeout (e.g., to 1-5 minutes) and memory (e.g., 512 MB or 1024 MB) depending on your expected file sizes.

## AWS IAM Permissions

Your Lambda function needs an execution role with permissions to read the source files from S3, write the processed JSON files back to S3, and write execution logs to CloudWatch.

Attach the following policy to your Lambda's IAM Role. **Make sure to replace `YOUR-BUCKET-NAME` with your actual S3 bucket name.**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET-NAME/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::YOUR-BUCKET-NAME/output/*"
            ]
        }
    ]
}
