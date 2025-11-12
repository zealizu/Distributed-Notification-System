import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import InvalidArgumentError
import json
import os
from dotenv import load_dotenv

load_dotenv()
FIREBASE_KEY = os.environ.get("FIREBASE_KEY") 

cred_dict = json.loads(FIREBASE_KEY)
cred = credentials.Certificate(cred_dict)
firebase_admin.initialize_app(cred)

def send_notification(push_token):
    # raise Exception("Simulated FCM failure")
    print("Sending Notification")
    message = messaging.Message(
        # 1. Fallback notification (for mobile / older clients)
        notification=messaging.Notification(
            title="title",
            body="this is a new notification",
            image="e"
        ),
        # 2. Web-specific rich notification
        webpush=messaging.WebpushConfig(
            notification=messaging.WebpushNotification(
                title="HNG WILL NOT BE THE END OF US",
                body="this is a new notification",
                image="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSBfZLfqCIbkDagoUMrIPr1wXSu3uqja01UJA&s",
                icon="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSBfZLfqCIbkDagoUMrIPr1wXSu3uqja01UJA&s",
            )
        ),

        # 3. Custom data + token
        data={
            "link": "https://google.com"
        },
        
        token= push_token
    )
    
    messaging.send(message)

    