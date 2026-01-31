from fastapi import FastAPI, Request
import requests, os

app = FastAPI()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
JOB_NAME = os.getenv("JOB_NAME")

@app.post("/telegram-webhook")
async def telegram_webhook(req: Request):
    data = await req.json()
    
    # Telegram user messages vs channel posts
    if "message" in data:
        message_text = data["message"]["text"]
    elif "channel_post" in data:
        message_text = data["channel_post"]["text"]
    else:
        # Ignore unknown updates
        return {"ok": True, "ignored": True}

    print("Received message:", message_text)

    # Trigger Databricks job
    jobs_list = requests.get(
        f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    ).json()
    job_id = next(job["job_id"] for job in jobs_list["jobs"] if job["settings"]["name"] == JOB_NAME)

    requests.post(
        f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
        json={"job_id": job_id, "notebook_params": {"telegram_text": message_text}}
    )

    return {"ok": True}