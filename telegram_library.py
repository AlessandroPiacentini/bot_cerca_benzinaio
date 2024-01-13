import requests

class TelegramLibrary:
    url_telegram_bot=""
    parametri = {"offset": 0}

    def __init__(self, api_key):
        self.url_telegram_bot = f"https://api.telegram.org/bot{api_key}/"
        
    def getUpdates(self):
        resp = requests.get(self.url_telegram_bot+"getUpdates", params=self.parametri)
        resp.raise_for_status()
        data = resp.json()
        
        if "result" in data and data["result"]:
            last_update_id = data["result"][-1]["update_id"]
            self.parametri["offset"] = last_update_id + 1
            
        return data
        
    def sendMassage(self, message, chat_id, reply_markup=None):
        requests.post(
            self.url_telegram_bot+"sendMessage",
            data={"chat_id": chat_id, "text": message, "reply_markup": reply_markup}
        )