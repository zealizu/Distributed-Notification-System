import firebase_admin
from firebase_admin import credentials, messaging
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import requests

load_dotenv()
FIREBASE_KEY = os.environ.get("FIREBASE_KEY")
STATUS_ENDPOINT = os.environ.get("STATUS_ENDPOINT")
STATUS_ENDPOINT_API = os.environ.get("STATUS_ENDPOINT_API") 

cred_dict = json.loads(FIREBASE_KEY)
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)

def send_notification(push_token, template_payload:dict, link):
    # raise Exception("Simulated FCM failure")
    print("Sending Notification")
    message = messaging.Message(
        # 1. Fallback notification (for mobile / older clients)
        notification=messaging.Notification(
            title= template_payload["title"],
            body=template_payload["body"],
        ),
        # 2. Web-specific rich notification
        webpush=messaging.WebpushConfig(
            notification=messaging.WebpushNotification(
                title=template_payload["title"],
                body=template_payload["body"],
                image=template_payload["image_url"],
                icon=template_payload["icon_url"],
            )
        ),

        # 3. Custom data + token
        data={
            "link": str(link)
        },
        
        token= push_token
    )
    messaging.send(message)
    
def update_notifcation_status(notification_id, status):
    payload = {
        "notification_id": notification_id.strip(),
        "status": status.strip().lower(),
        "timestamp": (datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    response = requests.post(url=f"{STATUS_ENDPOINT}/push/status", json=payload, headers={"x-api-key":STATUS_ENDPOINT_API})
    return response.json()


# img_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSc7OkzmIzPe3PfD87JriNQ9GxK9QNgxO6Lbg&s"
# icon_url = "https://hng.tech/media/images/blog/hng-logo.svg"
# dicti = {"title": "New Feature on Your Dashboard!",
#         "body": "We've rolled out a new feature to improve your experience. Check it out now!",
#         "image_url": img_url,
#         "icon_url": icon_url
#         }
# send_notification("d76xdWbulDNHUYWFMHemsj:APA91bG7LAVLZA11L9tsoY3ZvJY5zMKaLx7PkoATUytcTAq6Fx4XkTXO37knBc0V1DwQgvPvGOQk2AlYuvs2CGY4f5kvpv7fszRkxg-YNafJ5vOt6Vhu6U4",dicti,"https://google.com")