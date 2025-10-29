# worker/worker.py
from __future__ import annotations
import json, time, tempfile, logging, threading
from pathlib import Path

from botocore.exceptions import ClientError

from common.config import (
    assert_core_env,
    S3_BUCKET,
    SQS_URL,
    MAX_VIS_TIMEOUT,
    DDB_JOBS_TABLE,
)
from common.aws import s3, sqs, ddb_table
from .transcode import run_ffmpeg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")

_s3  = s3()
_sqs = sqs()
_jobs = ddb_table(DDB_JOBS_TABLE)

# Small helpers
def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _get_job(job_id: str) -> dict | None:
    return _jobs.get_item(Key={"jobId": job_id}).get("Item")

def _update_job(job_id: str, **fields) -> None:
    """
    Safe, generic SET update; ignores conditional failures (e.g., if job already DONE).
    """
    fields["updatedAt"] = _now()
    names = {f"#k{i}": k for i, k in enumerate(fields.keys(), 1)}
    vals  = {f":v{i}": v for i, v in enumerate(fields.values(), 1)}
    expr  = "SET " + ", ".join(f"{nk} = {vk}" for (nk, vk) in zip(names.keys(), vals.keys()))
    try:
        _jobs.update_item(
            Key={"jobId": job_id},
            UpdateExpression=expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=vals,
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            log.warning("Conditional update failed for job %s (likely already DONE). Ignoring.", job_id)
        else:
            raise

def _download_from_s3(key: str, dest_path: str) -> None:
    _s3.download_file(S3_BUCKET, key, dest_path)

def _upload_to_s3(src_path: str, key: str) -> None:
    _s3.upload_file(src_path, S3_BUCKET, key)

def _delete_msg(receipt_handle: str) -> None:
    _sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)

def _extend_visibility(receipt_handle: str, seconds: int) -> None:
    _sqs.change_message_visibility(
        QueueUrl=SQS_URL,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=seconds,
    )

class VisibilityExtender:
    """
    Background helper that periodically extends SQS visibility while work runs.
    """
    def __init__(self, receipt_handle: str, period_sec: int = 60):
        self.receipt_handle = receipt_handle
        self.period_sec = period_sec
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.wait(self.period_sec):
            try:
                _extend_visibility(self.receipt_handle, MAX_VIS_TIMEOUT)
                log.debug("Extended visibility by %ss", MAX_VIS_TIMEOUT)
            except Exception as e:
                log.warning("Visibility extension failed: %s", e)

    def start(self):
        self._t.start()

    def stop(self):
        self._stop.set()
        self._t.join(timeout=5)

# Core processing

def process_message(msg_body: dict, receipt_handle: str) -> None:
    job_id   = msg_body["jobId"]
    inputKey = msg_body["inputKey"]
    preset   = msg_body.get("preset", "mp4-720p")

    # Idempotency guard
    job = _get_job(job_id)
    if job and job.get("status") in {"RUNNING", "DONE"}:
        log.info("Job %s already %s — deleting message.", job_id, job["status"])
        _delete_msg(receipt_handle)
        return

    log.info("Starting job %s (preset=%s, input=%s)", job_id, preset, inputKey)
    _update_job(job_id, status="RUNNING", startedAt=_now())

    with tempfile.TemporaryDirectory() as td:
        local_in  = str(Path(td) / "input.mp4")
        local_out = str(Path(td) / "output.mp4")

        # 1) Download input from S3
        log.info("Downloading s3://%s/%s", S3_BUCKET, inputKey)
        _download_from_s3(inputKey, local_in)

        extender = VisibilityExtender(receipt_handle, period_sec=60)
        try:
            # Start background visibility extension in case encode is long
            extender.start()

            # 2) Transcode (single output per job — horizontal scale)
            run_ffmpeg(local_in, local_out, preset=preset, intensity="high")

            # 3) Upload to deterministic key (idempotent result)
            outputKey = f"output/{job_id}.mp4"
            log.info("Uploading to s3://%s/%s", S3_BUCKET, outputKey)
            _upload_to_s3(local_out, outputKey)

            # 4) Mark DONE and delete message
            _update_job(job_id, status="DONE", outputKey=outputKey, finishedAt=_now())
            _delete_msg(receipt_handle)
            log.info("Job %s complete.", job_id)

        except Exception as e:
            # Mark FAILED but do not delete the message; allow retry / DLQ redrive
            log.exception("Job %s failed: %s", job_id, e)
            _update_job(job_id, status="FAILED", error=str(e), finishedAt=_now())
        finally:
            # Always stop the extender
            try:
                extender.stop()
            except Exception:
                pass

def main() -> None:
    assert_core_env()
    log.info("Worker online; polling %s", SQS_URL)
    while True:
        resp = _sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,            # long polling
            VisibilityTimeout=MAX_VIS_TIMEOUT,
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue
        for m in msgs:
            try:
                body = json.loads(m["Body"])
            except Exception:
                # If message is malformed, drop it to avoid poison loops
                log.error("Malformed SQS message; deleting. Body=%r", m.get("Body"))
                _delete_msg(m["ReceiptHandle"])
                continue
            process_message(body, m["ReceiptHandle"])

if __name__ == "__main__":
    main()