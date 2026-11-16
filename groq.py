
import os
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv(".env", override=True)

api_key = os.getenv("GROQ_API_KEY", "").strip()
base_url = os.getenv(
    "LLM_BASE_URL",
    "https://api.groq.com/openai/v1"
).strip()
model = os.getenv(
    "LLM_MODEL",
    "openai/gpt-oss-20b"
).strip()

if not api_key:
    raise ValueError("GROQ_API_KEY is missing from .env")

# Initialise Groq client
client = OpenAI(
    api_key=api_key,
    base_url=base_url,
    timeout=60.0,
    max_retries=2
)

print("=" * 50)
print("       APPLYSENSE AI - GROQ CHAT")
print(f"       Model: {model}")
print("=" * 50)

while True:
    question = input("\nYou: ").strip()

    if question.lower() in ["exit", "quit", "stop"]:
        print("Chat ended. Goodbye!")
        break

    if not question:
        print("Please enter a question.")
        continue

    try:
        print("\nThinking...")

        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a helpful AI assistant. "
                        "Answer questions clearly and accurately."
                    )
                },
                {
                    "role": "user",
                    "content": question
                }
            ],
            max_completion_tokens=1500,
            temperature=0.3
        )

        answer = response.choices[0].message.content

        if answer and answer.strip():
            print("\nAssistant:", answer)
        else:
            print("\nNo text response generated.")
            print("Finish reason:", response.choices[0].finish_reason)
            print("Usage:", response.usage)

    except Exception as error:
        print("\nAPI Error:", type(error).__name__)
        print("Details:", str(error))
