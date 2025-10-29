import os

# Mandatory
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
S3_BUCKET = os.getenv("S3_BUCKET")  # e.g. a3-n1234567-videos
SQS_URL = os.getenv("SQS_URL")  # main queue
DLQ_URL = os.getenv("DLQ_URL")  # dead-letter queue (optional)
DDB_JOBS_TABLE = os.getenv("DDB_JOBS_TABLE", "a3_jobs") # DynamoDB table name

# App settings
JWT_SECRET      = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALG         = "HS256"
PRESIGN_EXP_SECS= int(os.getenv("PRESIGN_EXP_SECS", "900"))  # 15m links
MAX_VIS_TIMEOUT = int(os.getenv("MAX_VIS_TIMEOUT", "1800"))  # 30m; align with SQS queue setting

# Validation (fail early in API/Worker startup)
def assert_core_env():
    missing = [k for k,v in dict(
        S3_BUCKET=S3_BUCKET, SQS_URL=SQS_URL, DDB_JOBS_TABLE=DDB_JOBS_TABLE
    ).items() if not v]
    
    if missing:
        raise RuntimeError(f"Missing required env vars: {missing}")