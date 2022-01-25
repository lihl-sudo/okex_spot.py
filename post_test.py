import json
import requests


url = "http://116.77.75.254:80/buy"

params = {
    "side": "buy",
    "amount": "1"
}

resp = requests.post(url, json.dumps(params))
print(resp.json())