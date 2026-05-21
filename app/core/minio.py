import os
from pathlib import Path
from typing import Optional

import anyio
import boto3
from botocore.config import Config
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent.parent / ".env"
root_env_path = env_path.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
load_dotenv(dotenv_path=root_env_path, override=False)

MINIO_ENDPOINT = (os.getenv("MINIO_ENDPOINT") or "").strip()
MINIO_ACCESS_KEY = (os.getenv("MINIO_ACCESS_KEY") or "").strip()
MINIO_SECRET_KEY = (os.getenv("MINIO_SECRET_KEY") or "").strip()
MINIO_BUCKET = (os.getenv("MINIO_BUCKET") or "").strip()
MINIO_PUBLIC_BASE_URL = (os.getenv("MINIO_PUBLIC_BASE_URL") or "").strip().rstrip("/")

_s3 = None


def _client():
    global _s3
    if _s3 is None:
        if not (MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY and MINIO_BUCKET):
            raise RuntimeError("MinIO env is not configured (MINIO_ENDPOINT/MINIO_ACCESS_KEY/MINIO_SECRET_KEY/MINIO_BUCKET)")
        _s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=os.getenv("MINIO_REGION") or "us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
    return _s3


async def put_object(key: str, content: bytes, *, content_type: Optional[str] = None) -> str:
    def _do():
        kwargs = {"Bucket": MINIO_BUCKET, "Key": key, "Body": content}
        if content_type:
            kwargs["ContentType"] = content_type
        _client().put_object(**kwargs)

    await anyio.to_thread.run_sync(_do)
    if MINIO_PUBLIC_BASE_URL:
        return f"{MINIO_PUBLIC_BASE_URL}/{MINIO_BUCKET}/{key.lstrip('/')}"
    return f"{MINIO_ENDPOINT.rstrip('/')}/{MINIO_BUCKET}/{key.lstrip('/')}"


async def delete_objects(prefix: str) -> None:
    pfx = prefix.lstrip("/")

    def _do():
        s3 = _client()
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=pfx):
            for item in page.get("Contents") or []:
                k = item.get("Key")
                if k:
                    keys.append({"Key": k})
        if keys:
            for i in range(0, len(keys), 1000):
                s3.delete_objects(Bucket=MINIO_BUCKET, Delete={"Objects": keys[i : i + 1000]})

    await anyio.to_thread.run_sync(_do)
