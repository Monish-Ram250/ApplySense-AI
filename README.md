

# 🚀 ApplySense AI

### Agentic AI-Powered Recruitment Email Monitoring & Application Tracking System

ApplySense AI is an intelligent recruitment email monitoring assistant designed to automate the process of identifying, analysing, and tracking important recruitment-related emails.

It integrates Gmail with Large Language Models (LLMs) to understand recruitment communications, extract important information, classify hiring events, and deliver timely notifications through Telegram.

The system uses **Agentic AI concepts, LangGraph orchestration, structured LLM outputs, persistent storage, and automated notification workflows** to reduce the effort of manually monitoring recruitment emails.

> **Project Status:** 🛠️ Active Development\
> The core Agentic AI workflow has been developed and tested locally. AWS deployment is currently in progress, handled by Raghava Sai.

---

## ✨ Key Features

- **Gmail Integration:** Connects to Gmail using Google OAuth 2.0 with read-only permissions.
- **Automated Email Monitoring:** Retrieves emails from configured Gmail labels, including INBOX and SPAM.
- **AI-Powered Email Classification:** Uses Groq-hosted LLMs to understand and classify recruitment-related emails.
- **Information Extraction:** Extracts company names, job roles, interview schedules, deadlines, and recruitment updates.
- **Structured Output Validation:** Uses Pydantic to validate LLM-generated responses.
- **Agentic Workflow Orchestration:** Uses LangGraph to manage workflow execution and decision routing.
- **Privacy Filtering:** Filters email content before passing it to the LLM.
- **Persistent Storage:** Uses SQLite to maintain processed email records and workflow execution details.
- **Telegram Notifications:** Sends consolidated notifications for important recruitment updates.
- **Error Handling and Retries:** Handles API failures, rate limits, and notification delivery errors.
- **Observability:** Supports LangSmith tracing for workflow monitoring and debugging.
- **AWS Deployment:** Cloud deployment and infrastructure configuration are currently in progress.

---

## 🏗️ System Architecture

```
                    Gmail API
                        |
                        v
                Email Retrieval
                  INBOX + SPAM
                        |
                        v
              Processed Email Filter
                        |
                        v
                Privacy Filtering
                        |
                        v
              Groq LLM Classification
                        |
                        v
              Pydantic Validation
                        |
                        v
             LangGraph Orchestration
                        |
              +---------+---------+
              |         |         |
              v         v         v
            Notify     Skip     Review
              |
              v
         SQLite Database
              |
              v
      Telegram Notification
```

## ⚙️ How It Works

1. **Authentication:** The application authenticates with Gmail through Google OAuth 2.0.
2. **Email Retrieval:** Retrieves emails from configured Gmail labels.
3. **Duplicate Filtering:** Checks previously processed email records to avoid unnecessary reprocessing.
4. **Privacy Filtering:** Applies privacy-related filtering before AI analysis.
5. **LLM Analysis:** Groq LLM analyses email content and extracts recruitment-related information.
6. **Output Validation:** Pydantic validates the structured response.
7. **Workflow Routing:** LangGraph routes the email based on its classification:
   - **Notify:** Important recruitment events requiring attention.
   - **Skip:** Irrelevant or non-actionable emails.
   - **Review:** Uncertain classifications requiring further attention.
8. **Persistence:** Stores processing information and workflow execution records in SQLite.
9. **Notification:** Sends a consolidated Telegram digest containing important recruitment updates.

---

## 🛠️ Technology Stack

| Category             | Technologies          |
| -------------------- | --------------------- |
| Programming Language | Python                |
| LLM Provider         | Groq                  |
| LLM Integration      | OpenAI-compatible API |
| Agentic AI Framework | LangGraph             |
| Email Integration    | Gmail API             |
| Authentication       | Google OAuth 2.0      |
| Data Validation      | Pydantic              |
| Database             | SQLite                |
| Notification System  | Telegram Bot API      |
| Backend              | Flask                 |
| Observability        | LangSmith             |
| Version Control      | Git, GitHub           |
| Cloud Deployment     | AWS — In Progress     |

---

## 📂 Project Structure

The current implementation uses a single-file application architecture.

```
ApplySense-AI/
│
├── app.py
│
├── .env
├── .env.example
│
├── credentials.json
├── token.json
│
├── data/
│   └── appliesense.db
│
├── README.md
└── .gitignore
```

*The exact files may vary depending on your local configuration. Credentials, tokens, environment files, and private database files must not be committed.*

---

## 🚀 Local Installation and Setup

### 1. Clone the Repository

```
git clone https://github.com/Monish-Ram250/ApplySense-AI.git

cd ApplySense-AI
```

### 2. Create a Virtual Environment

```
python -m venv .venv
```

Activate on Windows:

```
.\.venv\Scripts\Activate.ps1
```

Activate on macOS/Linux:

```
source .venv/bin/activate
```

### 3. Install Dependencies

```
pip install flask requests python-dotenv langgraph openai pydantic typing-extensions google-api-python-client google-auth google-auth-oauthlib langsmith
```

### 4. Configure Environment Variables

Create a `.env` file in the project root.

```
# Groq Configuration
GROQ_API_KEY=your_groq_api_key
LLM_MODEL=openai/gpt-oss-20b
LLM_BASE_URL=https://api.groq.com/openai/v1

# Gmail OAuth Configuration
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
GMAIL_REFRESH_TOKEN=your_gmail_refresh_token

# Telegram Configuration
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id

# Email Processing Limits
GMAIL_MAX_MESSAGES_PER_LABEL=2
MAX_EMAILS_PER_RUN=2

# Optional LangSmith Configuration
LANGSMITH_TRACING=false
LANGSMITH_API_KEY=
LANGSMITH_PROJECT=ApplySense-AI

# Scheduler Configuration
SCHEDULER_INTERVAL_HOURS=4
```

Replace all placeholders with your own credentials.

**Never upload your `.env` file or secret credentials to GitHub.**

---

## 🔑 External Service Configuration

### Gmail API and OAuth

1. Create a project in Google Cloud Console.
2. Enable the Gmail API.
3. Configure the OAuth consent screen.
4. Create an OAuth 2.0 Client ID for a Desktop application.
5. Configure the Gmail read-only scope:

```
https://www.googleapis.com/auth/gmail.readonly
```

6. Complete the OAuth authentication flow.
7. Obtain the refresh token and configure the required environment variables.

### Groq API

1. Create an account on [Groq](https://console.groq.com/).
2. Generate an API key.
3. Configure the API key in `.env`.
4. Set the model name supported by your Groq account.

### Telegram Bot

1. Create a Telegram bot using [BotFather](https://t.me/BotFather).
2. Start a conversation with your bot using `/start`.
3. Retrieve your Telegram Chat ID.
4. Configure the bot token and Chat ID in `.env`.

### LangSmith

LangSmith can be configured for tracing and debugging workflow execution.

Configure the API key, project name, and appropriate regional endpoint if tracing is enabled.

---

## ▶️ Running the Application

### Run a Single Processing Cycle

```
python app.py --trigger cli
```

### Run the Local Scheduler

```
python app.py --loop
```

### Run Using EventBridge Trigger Mode

```
python app.py --trigger eventbridge
```

### Start the Flask Application

```
gunicorn "app:create_app()"
```

The Flask application entry point does not automatically start the scheduler loop. Scheduled execution must be configured separately.

---

## 🗄️ Database and Persistence

ApplySense AI currently uses SQLite for persistent storage.

The database supports:

- Processed email tracking.
- Workflow execution records.
- Notification delivery status.
- Processing summaries.
- Duplicate-processing prevention.

The database location can be configured using the `DB_PATH` environment variable.

For cloud deployment, persistent storage and reliable database access are important considerations.

---

## ☁️ AWS Deployment — In Progress

The project is currently transitioning from local execution towards AWS cloud deployment.

The core Agentic AI workflow has been developed and tested locally. AWS deployment activities are being handled separately.

### Deployment Responsibilities

**Raghava Sai — AWS Deployment**

- Working on AWS cloud deployment.
- Handling cloud infrastructure and deployment configuration.
- Working towards running the application workflow in the AWS environment.
- Addressing deployment-related configuration and cloud execution requirements.

**Akula Monish Ram — Agentic AI Workflow Development**

- Designed and developed the complete ApplySense AI workflow.
- Implemented Gmail API integration and OAuth authentication.
- Developed LLM-based recruitment email classification using Groq.
- Implemented Agentic AI workflow orchestration using LangGraph.
- Developed structured output validation using Pydantic.
- Implemented privacy filtering and email-processing logic.
- Integrated SQLite for persistent workflow records.
- Implemented Telegram notification delivery.
- Worked on retry mechanisms, error handling, and LangSmith observability.

> **Note:** AWS deployment is still in progress. The cloud deployment is not yet complete, and further updates will be added as development continues.

---

## 🔮 Future Enhancements

- Complete AWS deployment.
- Improve cloud-based scheduling and execution.
- Strengthen persistent storage and workflow recovery.
- Expand recruitment email classification and evaluation.
- Develop a dashboard for recruitment application tracking.
- Improve Human-in-the-Loop review capabilities.
- Extend monitoring and observability.
- Explore MCP integration for exposing application capabilities through standardised tools.

---

## 🔐 Security and Privacy

- Gmail access is configured with read-only OAuth permissions.
- API keys and credentials are maintained through environment variables.
- Email content is treated as untrusted input.
- Sensitive information must not be committed to version control.
- Logs should not expose API keys, OAuth tokens, or private email content.
- Important recruitment decisions and deadlines should always be verified against the original email.

---

## 👨‍💻 Project Contributors

### 1. Akula Monish Ram

**Role: Agentic AI Workflow Development**

Responsible for designing and developing the complete ApplySense AI workflow, including:

- Agentic AI architecture and workflow design.
- Gmail API integration.
- Groq LLM integration and email classification.
- LangGraph orchestration and decision routing.
- Pydantic structured output validation.
- Privacy filtering.
- SQLite persistence.
- Telegram notification integration.
- Error handling, retry mechanisms, and observability.

GitHub: [Monish-Ram250](https://github.com/Monish-Ram250)

### 2. Raghava Sai

**Role: AWS Deployment**

Responsible for the ongoing AWS deployment work, including cloud infrastructure configuration and deployment-related activities.

---

## 📌 Project Status

| Component                  | Status                |
| -------------------------- | --------------------- |
| Gmail OAuth Integration    | Completed             |
| Email Retrieval            | Implemented           |
| LLM Classification         | Implemented           |
| Pydantic Validation        | Implemented           |
| LangGraph Workflow         | Implemented           |
| SQLite Persistence         | Implemented           |
| Telegram Notifications     | Tested Successfully   |
| Error Handling and Retries | Implemented           |
| LangSmith Observability    | Integration Available |
| AWS Deployment             | In Progress           |

---

## ⚠️ Disclaimer

ApplySense AI is an actively developing personal productivity project. AI-generated classifications and extracted information may be incorrect or incomplete. Always verify important recruitment updates, interview schedules, and deadlines using the original email.

---

⭐ **If you find this project interesting, consider starring the repository!**

**Built with Python, Agentic AI, LangGraph, Groq, and a focus on automating recruitment workflows.**

Add a clear application overview
