# Kinesis Event Producer

Python producer for sending sample JSON events to an Amazon Kinesis Data Stream.

## Requirements

- Python 3.13 or compatible
- AWS CLI configured with a profile authorized to write to the stream
- An existing Kinesis Data Stream
- Permission to call `kinesis:PutRecord`

## Install dependencies

From the repository root:

```powershell
python -m pip install -r .\producers\requirements.txt
```

If Python is installed but not available as `python`, use its full executable path.

## Local test (no AWS calls)

Generate five sample events and display their partition keys:

```powershell
python .\producers\producer.py --dry-run --count 5
```

The dry-run mode generates events locally and does not send records to AWS.

## Send events to Kinesis

After the stream is available and AWS permissions are configured:

```powershell
python .\producers\producer.py --stream dev-data-stream --region us-east-1 --profile terraform-dev --count 10 --interval 0.5
```

## Options

| Option | Description | Default |
|---|---|---|
| `--stream` | Kinesis stream name | `dev-data-stream` |
| `--region` | AWS region | `us-east-1` |
| `--profile` | AWS CLI profile | `terraform-dev` |
| `--count` | Number of events | `10` |
| `--interval` | Delay between sends, in seconds | `0.5` |
| `--dry-run` | Generate events without calling AWS | Disabled |

Each event includes an ID, event type, user ID, UTC timestamp, and sample amount. Partition keys are selected from `web`, `mobile`, `api`, `store`, and `campaign`.

## Validation status

The Python syntax check and local dry-run have been validated. Successful delivery to Kinesis and S3 must be verified after AWS service access is available.
