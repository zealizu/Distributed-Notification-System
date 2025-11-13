def get_template_data(template_code, user_name):
    img_url = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSc7OkzmIzPe3PfD87JriNQ9GxK9QNgxO6Lbg&s"
    icon_url = "https://hng.tech/media/images/blog/hng-logo.svg"
    templates = {
        "WELCOME": {
            "title": "Welcome to HNG 👋",
            "body": f"Hi {user_name}, we're excited to have you on board! Explore your dashboard to get started.",
            "image_url": img_url,
            "icon_url": icon_url
            },
        "UPDATE":  {
            "title": "New Feature on Your Dashboard!",
            "body": "We've rolled out a new feature to improve your experience. Check it out now!",
            "image_url": img_url,
            "icon_url": icon_url
            },
        "REMINDER": {
            "title": f"Hey {user_name}, don't forget!",
            "body": f"Your event is coming up on soon. Tap to view details.",
            "image_url": img_url,
            "icon_url": icon_url
            }
    }
    return templates.get(template_code)
