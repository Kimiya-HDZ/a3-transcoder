# common/aws.py
import boto3
from botocore.config import Config
from .config import AWS_REGION

_session = None

# Global client config (tweak if needed)
_BOTO_CFG = Config(
    region_name=AWS_REGION,
    retries={"max_attempts": 5, "mode": "standard"},
    connect_timeout=5,
    read_timeout=60,
)

def session():
    global _session
    if _session is None:
        _session = boto3.session.Session(region_name=AWS_REGION)
    return _session

def s3():
    return session().client("s3", config=_BOTO_CFG)

def sqs():
    return session().client("sqs", config=_BOTO_CFG)

def ddb():
    # resource uses region from session; Config isn’t accepted here
    return session().resource("dynamodb")

def ddb_table(name: str):
    return ddb().Table(name)