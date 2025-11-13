from flask import Flask, jsonify
from flask_cors import CORS
from flasgger import Swagger
import redis
import os
from dotenv import load_dotenv

load_dotenv()
REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,password=REDIS_PASSWORD, ssl=True, decode_responses=True)

app = Flask(__name__)
swagger = Swagger(app, template={
    "info": {
        "title": "Notification System API",
        "description": "Handles notification delivery and retry logic with idempotency and circuit breaker.",
        "version": "1.0.0"
    },
    "basePath": "/"
})
CORS(app)


@app.route("/health", methods=["GET"])
def health():
    """
    Health Check Endpoint
    ---
    tags:
      - Monitoring
    summary: Check service and consumer health
    responses:
      200:
        description: Service is running normally
        schema:
          type: object
          properties:
            status:
              type: string
              example: healthy
            queue:
              type: string
              example: push.queue
            breaker_state:
              type: string
              example: closed
    """
    breaker_state = r.get("breaker_state") or "unknown"
    return jsonify({'status': 'healthy', 'queue': "push.queue", 'breaker_state': breaker_state}), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)