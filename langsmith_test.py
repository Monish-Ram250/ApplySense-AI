from dotenv import load_dotenv
from langsmith import Client
import os

load_dotenv(".env", override=True)

api_key = os.getenv("LANGSMITH_API_KEY", "").strip()
endpoint = os.getenv(
    "LANGSMITH_ENDPOINT",
    "https://aws.api.smith.langchain.com"
).strip()

print("API key loaded:", bool(api_key))
print("Key prefix:", api_key[:8] if api_key else "Missing")
print("Endpoint:", endpoint)

try:
    client = Client(api_key=api_key, api_url=endpoint)
    projects = list(client.list_projects(limit=1))
    print("LangSmith connection successful!")
    print("Projects retrieved:", len(projects))
except Exception as error:
    print("Connection failed:", type(error).__name__)
    print("Error:", error)