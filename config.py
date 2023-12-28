import json

try:
    with open("settings.json") as file:
        data = json.load(file)
        SELENIUM_SERVERS = [f"http://{SELENIUM_SERVER['IP']}:{SELENIUM_SERVER['PORT']}" for SELENIUM_SERVER in data["SELENIUM_SERVERS"]]
except Exception:
    SELENIUM_SERVERS = []
    