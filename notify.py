import firebase_admin
from firebase_admin import credentials, messaging
from firebase_admin.exceptions import InvalidArgumentError

cred = credentials.Certificate("/Users/mac/Desktop/flask/notify/firebaseKey.json")
firebase_admin.initialize_app(cred)

def send_notification():
    # raise Exception("Simulated FCM failure")
    print("Sending Notification")
    message = messaging.Message(
        # 1. Fallback notification (for mobile / older clients)
        notification=messaging.Notification(
            title="title",
            body="this is a new notification",
            image="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSBfZLfqCIbkDagoUMrIPr1wXSu3uqja01UJA&s"
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
        
        token="eXi6whn1GG7ZxLS9hY_keT:APA91bE2OC5K6YaBfVYzFKzTJjIgyOlcwuUllSaejhfqX3lyrTf-Ocj5cWf0ra1IT423gLo43Pz_jYrOGtwuTJajtTmREh4qdnVttQ0dXuLkY-oPsirl9gM"
    )
    
    messaging.send(message)

    