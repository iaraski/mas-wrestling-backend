import os
from urllib.parse import quote
from uuid import UUID, uuid4

import anyio
import boto3
import httpx
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import Response

from app.authorization import require_staff_user_id
from app.core.minio import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    put_object as _minio_put_object,
)
from app.core.rest import rest_get
from app.core.telegram import get_telegram_file_url


router = APIRouter(prefix="/applications", tags=["applications"])

_minio_s3 = None


def _minio_client():
    global _minio_s3
    if _minio_s3 is None:
        if not (MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY and MINIO_BUCKET):
            raise RuntimeError("MinIO env is not configured")
        # Force direct MinIO access even when a local HTTP proxy is configured in the shell.
        _minio_s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=os.getenv("MINIO_REGION") or "us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, proxies={}),
        )
    return _minio_s3


def _supabase_headers() -> dict[str, str] | None:
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY") or "").strip()
    if not key:
        return None
    return {"apikey": key, "authorization": f"Bearer {key}"}


@router.get("/photo/{file_id}")
async def get_photo_proxy(file_id: str):
    real_url = await get_telegram_file_url(file_id)
    if not real_url:
        raise HTTPException(status_code=404, detail="Photo not found in Telegram")

    async with httpx.AsyncClient(timeout=15.0, http2=False) as client:
        try:
            response = await client.get(real_url)
            if response.status_code == 200:
                return Response(
                    content=response.content,
                    media_type=response.headers.get("Content-Type", "image/jpeg"),
                    headers={"Cache-Control": "public, max-age=3600"},
                )
            raise HTTPException(status_code=response.status_code, detail="Failed to download photo")
        except Exception as e:
            import traceback

            print(f"[Photo Proxy] Error downloading: {e}")
            print(traceback.format_exc())
            raise HTTPException(status_code=500, detail="Error downloading photo")


@router.get("/photo-key/{key:path}")
async def get_photo_key_proxy(key: str):
    normalized_key = str(key or "").lstrip("/")
    if not normalized_key.startswith("documents/"):
        raise HTTPException(status_code=404, detail="Not found")

    def _get_from_minio():
        try:
            result = _minio_client().get_object(Bucket=MINIO_BUCKET, Key=normalized_key)
            return result.get("Body").read(), result.get("ContentType")
        except ClientError as e:
            code = str((e.response or {}).get("Error", {}).get("Code") or "")
            if code in {"NoSuchKey", "NoSuchObject", "404"}:
                return None
            raise

    minio_res = await anyio.to_thread.run_sync(_get_from_minio)
    if minio_res is not None:
        content, content_type = minio_res
        return Response(
            content=content,
            media_type=content_type or "application/octet-stream",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    supabase_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    headers = _supabase_headers()
    if not supabase_url or not headers:
        raise HTTPException(status_code=404, detail="Not found")

    encoded = quote(normalized_key, safe="/")
    url = f"{supabase_url}/storage/v1/object/avatars/{encoded}"
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, http2=False) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=404, detail="Not found")
        content = resp.content
        content_type = (resp.headers.get("content-type") or "").split(";", 1)[0].strip() or None

    try:
        await _minio_put_object(normalized_key, content, content_type=content_type)
    except Exception:
        pass

    return Response(
        content=content,
        media_type=content_type or "application/octet-stream",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.post("/{app_id}/passport/photo")
async def upload_passport_photo(
    app_id: UUID,
    photo: UploadFile = File(...),
    _: str = Depends(require_staff_user_id),
):
    if not photo.content_type or not str(photo.content_type).startswith("image/"):
        raise HTTPException(status_code=400, detail="Only image files are supported")
    content = await photo.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large (max 10MB)")

    ext = os.path.splitext(photo.filename or "")[1].lower()
    if not ext:
        content_type = str(photo.content_type or "").lower()
        if content_type == "image/png":
            ext = ".png"
        elif content_type == "image/webp":
            ext = ".webp"
        else:
            ext = ".jpg"

    app_resp = await rest_get(
        "applications",
        {"select": "athlete_id", "id": f"eq.{str(app_id)}", "limit": "1"},
        write=True,
    )
    app_rows = app_resp.json()
    if not isinstance(app_rows, list) or not app_rows or not app_rows[0].get("athlete_id"):
        raise HTTPException(status_code=404, detail="Application not found")
    athlete_id = str(app_rows[0]["athlete_id"])

    object_path = f"documents/{athlete_id}/{uuid4().hex}{ext}"
    from app.core.minio import put_object

    photo_url = await put_object(
        object_path,
        content,
        content_type=photo.content_type or "application/octet-stream",
    )
    return {"ok": True, "photo_url": photo_url}
