# from fastapi import FastAPI, Request
# import os
# import httpx

# app = FastAPI()

# DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
# DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
# JOB_NAME = os.getenv("JOB_NAME")

# @app.post("/telegram-webhook")
# async def telegram_webhook(req: Request):
#     data = await req.json()

#     # Telegram message may be in 'message' or 'channel_post'
#     message_text = data.get("message", data.get("channel_post", {})).get("text", "")
#     if not message_text:
#         return {"ok": True, "note": "No text found"}

#     async with httpx.AsyncClient() as client:
#         # Get job list
#         jobs_list_resp = await client.get(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
#         )
#         jobs_list = jobs_list_resp.json()
#         job_id = next(
#             job["job_id"]
#             for job in jobs_list["jobs"]
#             if job["settings"]["name"] == JOB_NAME
#         )

#         # Run job
#         await client.post(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
#             json={"job_id": job_id, "notebook_params": {"telegram_text": message_text}}
#         )

#     return {"ok": True}

from fastapi import FastAPI, Request
import os
import httpx

app = FastAPI()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
JOB_NAME = os.getenv("JOB_NAME")

@app.post("/telegram-webhook")
async def telegram_webhook(req: Request):
    data = await req.json()

    # Telegram message may be in 'message' or 'channel_post'
    message_text = data.get("message", data.get("channel_post", {})).get("text", "")
    if message_text:
        print("🔔 Telegram webhook received channel message:", message_text)

    async with httpx.AsyncClient() as client:
        # Get job list
        jobs_list_resp = await client.get(
            f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        )
        jobs_list = jobs_list_resp.json()
        job_id = next(
            job["job_id"]
            for job in jobs_list.get("jobs", [])
            if job["settings"]["name"] == JOB_NAME
        )

        # Run job without passing message content to avoid consuming it
        await client.post(
            f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={"job_id": job_id}
        )

    return {"ok": True, "note": "Databricks job triggered, Telegram message untouched"}