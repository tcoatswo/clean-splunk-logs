# clean-splunk-logs
This is a robust AWS Lambda function designed to act as an automated pipeline that triggers when a compressed Splunk journal file (journal.zst) is uploaded to an S3 bucket. It decompresses it, scrubs the data to find valid JSON, and then saves the formatted JSON back to S3.
