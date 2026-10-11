# Splunk Journal ZST Processor (AWS Lambda)

This AWS Lambda function automatically processes compressed Splunk journal files (`journal.zst`) uploaded to an Amazon S3 bucket. It decompresses the files using Zstandard, extracts and validates the underlying JSON data, and saves the formatted JSON back to the S3 bucket. 

This is specifically designed to strip proprietary binary data from .zst files, yielding clean JSON output.

## Features

* **Automated Triggering:** Runs automatically via S3 PutObject events.
* **Zstandard Decompression:** Decompresses `.zst` files and falls back to scanning the raw file if decompression fails.
* **Robust Data Extraction:** Removes control bytes and finds the first valid JSON object, including nested objects and braces inside strings.
* **Smart Routing:** Parses the internal JSON to generate dynamic, descriptive output filenames based on `eventName` and `eventSource`.
* **Safe Logging and Cleanup:** Logs file metadata without JSON previews and removes temporary files after success or failure.

## Prerequisites

* AWS Account with permissions to create Lambda functions, IAM roles, and S3 buckets.
* A supported Python Lambda runtime.
* The `zstandard` Python package (bundled with your deployment package or added as a Lambda Layer).

## Environment Variables

You can configure the behavior of the Lambda function using the following environment variable:

| Variable | Description | Default Value |
| :--- | :--- | :--- |
| `OUTPUT_PREFIX` | The S3 folder/prefix where the processed JSON files will be saved. | `output/` |

## Deployment Instructions

1. **Package the Code:** Build the dependency on an environment compatible with your Lambda runtime and architecture.
   ```bash
   mkdir package
   pip install -r requirements.txt -t package
   cp zst_to_json.py package/
   cd package && zip -r ../function.zip .
   ```
2. **Create the Lambda Function:** Upload `function.zip` to AWS Lambda and set the handler to `zst_to_json.lambda_handler`.
3. **Configure S3 Trigger:** Go to your S3 bucket and configure an Event Notification to trigger this Lambda function on `s3:ObjectCreated:*` events.
   > **Tip:** Add a suffix filter for `.zst` to prevent the Lambda from triggering unnecessarily.
4. **Adjust Timeout, Memory, and Temporary Storage:** The function holds the downloaded file and decompressed text in memory while extracting JSON. Size these settings for your largest expected journal, and test with representative files.

Run the unit tests with `python -m unittest discover -s tests` after installing `requirements.txt` and `boto3` locally.

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
