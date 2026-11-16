
import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(".env")

with open("token.json", "r", encoding="utf-8") as file:
    token = json.load(file)

env_client_id = os.getenv("GOOGLE_CLIENT_ID", "").strip()
env_client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
env_refresh_token = os.getenv("GMAIL_REFRESH_TOKEN", "").strip()

print("Client ID matches:", env_client_id == token.get("client_id"))
print("Client Secret matches:", env_client_secret == token.get("client_secret"))
print("Refresh token matches:", env_refresh_token == token.get("refresh_token"))
print("Refresh token exists in token.json:", bool(token.get("refresh_token")))
print("Token file exists:", Path("token.json").exists())
