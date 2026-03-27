import os
import httpx
from typing import Optional

# This will be evaluated when the module is imported
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    print("[Telegram Core] WARNING: BOT_TOKEN is not set in environment variables!")
else:
    masked = f"{BOT_TOKEN[:5]}...{BOT_TOKEN[-5:]}" if len(BOT_TOKEN) > 10 else "***"
    print(f"[Telegram Core] BOT_TOKEN loaded: {masked}")

async def get_telegram_file_url(file_id: str) -> Optional[str]:
    if not BOT_TOKEN:
        print("[Telegram API] ERROR: BOT_TOKEN is not set!")
        return None
        
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile"
    payload = {"file_id": file_id}
    
    async with httpx.AsyncClient(timeout=15.0, http2=False) as client:
        try:
            response = await client.get(url, params=payload)
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    file_path = data["result"]["file_path"]
                    return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            else:
                print(f"[Telegram API] Error getting file: {response.text}")
        except Exception as e:
            print(f"[Telegram API] Error: {e}")
            
async def send_telegram_notification(chat_id: int, text: str):
    if not BOT_TOKEN:
        print("[Telegram Notification] ERROR: BOT_TOKEN is not set!")
        return
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    
    async with httpx.AsyncClient(timeout=15.0, http2=False) as client:
        try:
            response = await client.post(url, json=payload)
            if response.status_code != 200:
                print(f"[Telegram Notification] Error: {response.status_code} - {response.text}")
            else:
                print(f"[Telegram Notification] Sent to {chat_id}: {text[:50]}...")
        except Exception as e:
            print(f"[Telegram Notification] Critical Error: {e}")
