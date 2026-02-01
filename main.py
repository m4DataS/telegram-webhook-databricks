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


################## V2 without msg
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
#     if message_text:
#         print("🔔 Telegram webhook received channel message:", message_text)

#     async with httpx.AsyncClient() as client:
#         # Get job list
#         jobs_list_resp = await client.get(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
#         )
#         jobs_list = jobs_list_resp.json()
#         job_id = next(
#             job["job_id"]
#             for job in jobs_list.get("jobs", [])
#             if job["settings"]["name"] == JOB_NAME
#         )

#         # Run job without passing message content to avoid consuming it
#         await client.post(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
#             json={"job_id": job_id}
#         )

#     return {"ok": True, "note": "Databricks job triggered, Telegram message untouched"}




# ################## V3 With Msg specialy designed for the notebook
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
#     if message_text:
#         print("🔔 Telegram webhook received channel message:", message_text)

#     async with httpx.AsyncClient() as client:
#         # Get job list
#         jobs_list_resp = await client.get(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
#         )
#         jobs_list = jobs_list_resp.json()
#         job_id = next(
#             job["job_id"]
#             for job in jobs_list.get("jobs", [])
#             if job["settings"]["name"] == JOB_NAME
#         )

#         # Run Databricks job **passing the message as notebook parameter**
#         await client.post(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
#             json={
#                 "job_id": job_id,
#                 "notebook_params": {"telegram_message": message_text}
#             }
#         )

#     return {"ok": True, "note": "Databricks job triggered, Telegram message passed safely"}



# ################## V4 With Msg & full json
# from fastapi import FastAPI, Request
# import os
# import httpx

# app = FastAPI()

# DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
# DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
# JOB_NAME = os.getenv("JOB_NAME")


# def extract_telegram_message(update: dict) -> dict | None:
#     """
#     Normalize Telegram updates (message / channel_post / edited_channel_post)
#     into a single schema.
#     """
#     msg = (
#         update.get("message")
#         or update.get("channel_post")
#         or update.get("edited_channel_post")
#     )

#     if not msg:
#         return None

#     chat = msg.get("chat", {})

#     return {
#         "update_id": update.get("update_id"),
#         "message_id": msg.get("message_id"),
#         "channel_id": chat.get("id"),
#         "channel_title": chat.get("title"),
#         "text": msg.get("text"),
#         "date": msg.get("date"),
#     }


# @app.post("/telegram-webhook")
# async def telegram_webhook(req: Request):
#     update = await req.json()

#     normalized = extract_telegram_message(update)

#     if not normalized or not normalized.get("text"):
#         # Ignore non-text updates (joins, pins, etc.)
#         return {"ok": True, "status": "ignored"}

#     print("🔔 Telegram webhook received:")
#     print(normalized)

#     async with httpx.AsyncClient() as client:
#         # 1️⃣ Get Databricks job id
#         jobs_list_resp = await client.get(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
#         )
#         jobs_list_resp.raise_for_status()

#         jobs_list = jobs_list_resp.json()

#         job_id = next(
#             job["job_id"]
#             for job in jobs_list.get("jobs", [])
#             if job["settings"]["name"] == JOB_NAME
#         )

#         # 2️⃣ Trigger Databricks job
#         await client.post(
#             f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
#             headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
#             json={
#                 "job_id": job_id,
#                 # Optional: keep passing message if you want
#                 "notebook_params": {
#                     "telegram_message": normalized["text"]
#                 }
#             }
#         )

#     return {
#         "ok": True,
#         "note": "Databricks job triggered",
#         "message": normalized["text"]
#     }



################### V5 : must read large messages
from fastapi import FastAPI, Request
import os
import httpx
import json
import base64

app = FastAPI()

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_INSTANCE")
JOB_NAME = os.getenv("JOB_NAME")

@app.post("/telegram-webhook")
async def telegram_webhook(req: Request):
    data = await req.json()

    # This will show the pretty-printed JSON in your Railway logs
    print("🔔 Received Telegram JSON:", json.dumps(data, indent=2, ensure_ascii=False))
    
    # 1. Convert dict to JSON string
    json_str = json.dumps(data, ensure_ascii=False)
    
    # 2. Encode to Base64 (This turns "données" into safe ASCII like "ZG9ubsOpZXM=")
    encoded_param = base64.b64encode(json_str.encode("utf-8")).decode("ascii")


    # 1. Stringify the data
    # We must ensure this is a single string for the Databricks parameter

    async with httpx.AsyncClient(timeout=60.0) as client:
        # Get job list (Consider hardcoding JOB_ID to speed this up!)
        jobs_list_resp = await client.get(
            f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
        )
        jobs_list = jobs_list_resp.json()
        job_id = next(job["job_id"] for job in jobs_list.get("jobs", []) if job["settings"]["name"] == JOB_NAME)

        # 2. Trigger Job
        response = await client.post(
            f"{DATABRICKS_INSTANCE}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
            json={
                "job_id": int(job_id),
                "notebook_params": {
                    "telegram_message": encoded_param # Send the encoded string
                }
            }
        )
        
        # This will print WHY it failed (e.g., "Parameter 'telegram_message' is too long")
        if response.status_code != 200:
            print(f"❌ Databricks Error Detail: {response.text}")
        else:
            print(f"🚀 Success! Run ID: {response.json().get('run_id')}")

    return {"ok": True}