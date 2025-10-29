# api/main.py
from __future__ import annotations
import uuid
from typing import Dict, Any
import json

from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .auth import get_current_user, validate_login, create_access_token, USERS
from .presign import presigned_put, presigned_get
from .dal import put_job, get_job
from common.aws import sqs
from common.config import assert_core_env, SQS_URL

app = FastAPI(title="CAB432 A3 Video Transcoder")

@app.on_event("startup")
def _startup():
    assert_core_env()

# serve the existing static UI
app.mount("/static", StaticFiles(directory="api/static"), name="static")

# ----------------- Health / Root -----------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    # if you want to serve an index, keep a simple message or point to /static
    return {"message": "A3 Transcoder API. See /static for UI."}

# ----------------- Auth -----------------
class LoginReq(BaseModel):
    username: str
    password: str

@app.get("/auth/me")
def me(user: Dict[str, Any] = Depends(get_current_user)):
    return user

@app.post("/auth/login")
def login(req: LoginReq):
    if not validate_login(req.username, req.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    role = USERS[req.username]["role"]
    token = create_access_token(req.username, role)
    return {"access_token": token, "token_type": "bearer", "role": role}

# ----------------- Upload: pre-signed URL -----------------
class UploadUrlReq(BaseModel):
    filename: str = Field(..., example="myvideo.mp4")
    contentType: str = Field("video/mp4")

class UploadUrlRes(BaseModel):
    key: str
    uploadUrl: str

@app.post("/videos/upload-url", response_model=UploadUrlRes)
def get_upload_url(req: UploadUrlReq, user=Depends(get_current_user)):
    key = f"input/{uuid.uuid4()}-{req.filename}"
    url = presigned_put(key, req.contentType)
    return {"key": key, "uploadUrl": url}

# ----------------- Transcode request -----------------
class TranscodeReq(BaseModel):
    inputKey: str
    preset: str = Field("mp4-720p", description="mp4-720p or mp4-1080p")

class TranscodeRes(BaseModel):
    jobId: str
    status: str

@app.post("/transcode", response_model=TranscodeRes)
def transcode(req: TranscodeReq, user=Depends(get_current_user)):
    """
    Creates a Job in DynamoDB and enqueues an SQS message for the worker.
    """
    assert_core_env()
    job_id = str(uuid.uuid4())
    # Initial job record
    put_job({
        "jobId": job_id,
        "userId": user["username"],
        "inputKey": req.inputKey,
        "preset": req.preset,
        "status": "PENDING",
    })
    # Enqueue message
    body = {"jobId": job_id, "inputKey": req.inputKey, "preset": req.preset, "userId": user["username"]}
    sqs().send_message(
        QueueUrl=SQS_URL,
        MessageBody=json.dumps(body),
        MessageAttributes={"userId": {"StringValue": user["username"], "DataType": "String"}}
    )
    return {"jobId": job_id, "status": "PENDING"}

# ----------------- Job status -----------------
@app.get("/jobs/{job_id}")
def job_status(job_id: str, user=Depends(get_current_user)):
    item = get_job(job_id)
    if not item or item.get("userId") != user["username"]:
        raise HTTPException(status_code=404, detail="Job not found")
    if item.get("status") == "DONE" and item.get("outputKey"):
        item["downloadUrl"] = presigned_get(item["outputKey"])
    return item