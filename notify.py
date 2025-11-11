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
            body="this is a new notification"
        ),
        # 2. Web-specific rich notification
        webpush=messaging.WebpushConfig(
            notification=messaging.WebpushNotification(
                title="title",
                body="this is a new notification",
                image="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSBfZLfqCIbkDagoUMrIPr1wXSu3uqja01UJA&s",
                icon="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSBfZLfqCIbkDagoUMrIPr1wXSu3uqja01UJA&s",
            )
        ),

        # 3. Custom data + token
        data={
            "link": "https://google.com"
        },
        
        token="djQe-Uog-jsWBgK_wfQBAm:APA91bHrpCd0D3Wbcw9oQN8zY6Bp7w_dsBFPHJtQWDZ3jCnkqzYZeeRB5K4mhtVMMSGE5WlRaiEkfWx1pT1PkJ5gSVioS5O3QoVls1_-JozyXO96SmrnXwU"
    )
    
    messaging.send(message)

    