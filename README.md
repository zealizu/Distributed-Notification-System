# Distributed Notification System

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-2.0%2B-green)](https://flask.palletsprojects.com/)

## A robust, distributed notification system built with **Python**, **Flask**, **RabbitMQ**, and **Redis**, featuring **push notifications**, **retry logic**, **idempotency**, **circuit breaker**, and **dead-letter queues**. This system is designed to handle high-throughput notification delivery in a reliable and fault-tolerant manner.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)

  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Environment Variables](#environment-variables)
  - [Running the Application](#running-the-application)

- [API Documentation](#api-documentation)
- [Usage](#usage)
- [CI/CD Pipeline](#cicd-pipeline)
- [Contributing](#contributing)

---

## Features

- **Push Notifications**: Send notifications to devices via Firebase Cloud Messaging (FCM).
- **Retry Logic**: Automatic exponential backoff retry for failed deliveries.
- **Idempotency**: Ensures the same notification is not processed multiple times using Redis.
- **Circuit Breaker**: Protects the system from repeated failures using `pybreaker`.
- **Dead-Letter Queue**: Failed messages are routed to a separate queue for later inspection.
- **Queue Management**: Built with RabbitMQ, supports durable queues and direct exchange routing.
- **Flask API**: Health check endpoint and easily extendable to other endpoints.
- **Swagger / OpenAPI**: Provides API documentation for easy testing.

---

## Architecture

```
Client / Service
       |
       v
   API Endpoint
       |
       v
RabbitMQ (push.queue / email.queue)
       |
       v
 Notification Consumer (Python + pika)
       |
       v
Redis (Idempotency + Retry Tracking)
       |
       v
Firebase Cloud Messaging
       |
       v
Device
```

- **RabbitMQ** manages message delivery and retries.
- **Redis** tracks retries and ensures idempotency.
- **Firebase Admin SDK** sends notifications to devices.

---

## Tech Stack

- Python 3.13+
- Flask
- Flask-CORS
- Firebase Admin SDK
- RabbitMQ (AMQP)
- Redis
- Pika (Python RabbitMQ client)
- PyBreaker (Circuit breaker)
- Pydantic (Data validation)
- Requests (HTTP client)
- Flasgger (Swagger/OpenAPI documentation)

---

## Getting Started

### Prerequisites

- Python 3.13+
- Redis instance (local or hosted, e.g., Aiven)
- RabbitMQ instance (local or hosted, e.g., CloudAMQP)
- Firebase service account key

---

### Installation

1. Clone the repository:

```bash
git clone https://github.com/zealizu/Distributed-Notification-System.git
cd Distributed-Notification-System
```

2. Create a virtual environment and activate it:

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

---

### Environment Variables

Create a `.env` file in the root directory:

```env
RABBITMQ_HOST=<your_rabbitmq_url>
USER_ENDPOINT=<your_user_service_url>
TEMPLATE_ENDPOINT=<your_template_service_url>
FIREBASE_KEY=<your_firebase_json_string>
REDIS_HOST=<your_redis_host>
REDIS_PORT=<your_redis_port>
REDIS_PASSWORD=<your_redis_password>
```

---

### Running the Application

```bash
python app.py
```

- Health check endpoint: `GET /health`
- Swagger UI: `http://localhost:5050/apidocs/`

---

## API Documentation

### Health Endpoint

```
GET /health
```

**Response**

```json
{
  "status": "healthy",
  "queue": "push.queue",
  "breaker_state": "closed"
}
```

### Push Notification Endpoint

- Supports sending push notifications to users.
- Payload example:

```json
{
  "notification_type": "push",
  "user_id": "user_id1",
  "template_code": "PRODUCT_UPDATE",
  "variables": {
    "name": "Alex Smith",
    "link": "https://example.com/view",
    "meta": {}
  },
  "request_id": "req-push-67892",
  "priority": 1,
  "metadata": {}
}
```

---

## Usage

1. Add users to your database with `push_token` and preferences.
2. Send notification payloads to RabbitMQ `push.queue`.
3. Consumers will pick up messages, handle retries, and ensure idempotency.
4. Failed messages are moved to `failed.queue` for inspection.

---

## CI/CD Pipeline

- The project uses **GitHub Actions** for CI:

  - Runs on push to `master` branch.
  - Sets up Python environment.
  - Installs dependencies.
  - Runs unit tests.

- No Docker required for CI.
- Secrets are stored in GitHub Actions for safe environment variable management.

Example workflow file: `.github/workflows/ci.yml`

---

## Contributing

1. Fork the repository.
2. Create a branch: `git checkout -b feature/my-feature`.
3. Commit your changes: `git commit -m "Add new feature"`.
4. Push to branch: `git push origin feature/my-feature`.
5. Open a Pull Request
