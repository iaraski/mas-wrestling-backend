import os
import json
import shutil
import subprocess
import httpx
from typing import Optional, Any

# This will be evaluated when the module is imported
BOT_TOKEN = os.getenv("BOT_TOKEN")
TELEGRAM_API_BASE = (os.getenv("TELEGRAM_API_BASE") or "https://api.telegram.org").rstrip("/")
APP_DEBUG = os.getenv("APP_DEBUG") == "1"

if not BOT_TOKEN:
    print("[Telegram Core] WARNING: BOT_TOKEN is not set in environment variables!")
else:
    if APP_DEBUG:
        masked = f"{BOT_TOKEN[:5]}...{BOT_TOKEN[-5:]}" if len(BOT_TOKEN) > 10 else "***"
        print(f"[Telegram Core] BOT_TOKEN loaded: {masked}")

async def get_telegram_file_url(file_id: str) -> Optional[str]:
    if not BOT_TOKEN:
        print("[Telegram API] ERROR: BOT_TOKEN is not set!")
        return None

    url = f"{TELEGRAM_API_BASE}/bot{BOT_TOKEN}/getFile"
    payload = {"file_id": file_id}
    
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, http2=False) as client:
        try:
            response = await client.get(url, params=payload)
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    file_path = data["result"]["file_path"]
                    return f"{TELEGRAM_API_BASE}/file/bot{BOT_TOKEN}/{file_path}"
                print(f"[Telegram API] getFile not ok: {data}")
            else:
                print(f"[Telegram API] getFile HTTP {response.status_code}: {response.text}")
        except httpx.ConnectError:
            pass
        except Exception as e:
            print(f"[Telegram API] getFile exception {type(e).__name__}: {e!r}")

    curl = shutil.which("curl") or shutil.which("curl.exe")
    if not curl:
        print("[Telegram API] curl is not available for fallback")
        return None
    try:
        proc = subprocess.run(
            [curl, "-sS", "--max-time", "30", "-G", url, "--data-urlencode", f"file_id={file_id}"],
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            err = (proc.stderr or b"")[:200].decode("utf-8", "ignore")
            print(f"[Telegram API] getFile curl failed rc={proc.returncode} err={err}")
            return None
        data = json.loads((proc.stdout or b"").decode("utf-8", "ignore"))
        if data.get("ok") and data.get("result", {}).get("file_path"):
            file_path = data["result"]["file_path"]
            return f"{TELEGRAM_API_BASE}/file/bot{BOT_TOKEN}/{file_path}"
        print(f"[Telegram API] getFile curl not ok: {data}")
        return None
    except Exception as e:
        print(f"[Telegram API] getFile curl exception {type(e).__name__}: {e!r}")
        return None
            
async def send_telegram_notification(chat_id: int, text: str, reply_markup: Any | None = None):
    if not BOT_TOKEN:
        print("[Telegram Notification] ERROR: BOT_TOKEN is not set!")
        return
    
    url = f"{TELEGRAM_API_BASE}/bot{BOT_TOKEN}/sendMessage"
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, http2=False) as client:
        try:
            response = await client.post(url, json=payload)
            if response.status_code != 200:
                print(f"[Telegram Notification] Error: {response.status_code} - {response.text}")
            else:
                if APP_DEBUG:
                    print(f"[Telegram Notification] Sent to {chat_id}: {text[:50]}...")
        except Exception as e:
            print(f"[Telegram Notification] Critical Error {type(e).__name__}: {e!r}")
