# 🚀 telegram-webhook-databricks

### Event-driven Telegram → Databricks LLM pipeline using webhooks and Delta Lake

This project demonstrates a cost-efficient, event-driven architecture that connects Telegram messages to Databricks for real-time ingestion, processing, and LLM-powered responses.

Instead of running always-on streaming jobs, the pipeline is triggered only when new Telegram messages arrive, making it suitable for low-cost or free-tier environments.

## 🧠 Architecture Overview

1. Telegram Channel → Webhook → Databricks → LLM → Telegram Reply
 * Telegram Bot Webhook
 * Hosted on Railway
 * Receives channel messages in real time
 * No polling, no long-running jobs
2. Raw Data Storage
 * Incoming messages are stored as JSON files
 * Written to a Databricks Volume (raw_telegram)
3. Bronze Layer (Delta Lake)
 * Databricks Autoloader ingests new files on arrival
 * Schema evolution & checkpointing handled automatically
4. Silver Layer (LLM Processing)
 * Triggered when Bronze table updates
 * Sends messages to a Databricks-hosted LLM
 * Stores AI responses in a Silver Delta table
 * Replies back to Telegram via Bot API
5. Fully Event-Driven
 * No infinite streaming loops
 * Compatible with Databricks Free Edition constraints

## ✨ Key Features
* 🔔 Webhook-based triggering (no polling)
* 🧱 Medallion architecture (Raw → Bronze → Silver)
* 🤖 LLM inference using Databricks models
* 💬 Automatic Telegram replies
* 💾 Delta Lake with exactly-once semantics
* 💸 Cost-optimized for free / low-tier environments

## 🛠️ Tech Stack
* Telegram Bot API
* Railway (Webhook hosting)
* Databricks
   * Delta Lake
   * Autoloader
   * Jobs & Triggers
* Python / PySpark
* LLM inference via Databricks endpoints

## 🎯 Why this project?
This project showcases:
* Real-world event-driven data engineering
* Practical LLM integration in production pipelines
* Cost-aware architecture decisions
* End-to-end ownership from ingestion to AI output
It’s designed as a portfolio project for Data / Platform / MLOps / DevOps roles.