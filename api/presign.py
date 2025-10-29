from common.aws import s3
from common.config import S3_BUCKET, PRESIGN_EXP_SECS

def presigned_put(key: str, content_type: str = "application/octet-stream"):
    return s3().generate_presigned_url(
        "put_object",
        Params={"Bucket": S3_BUCKET, "Key": key, "ContentType": content_type},
        ExpiresIn=PRESIGN_EXP_SECS,
    )

def presigned_get(key: str):
    return s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=PRESIGN_EXP_SECS,
    )
