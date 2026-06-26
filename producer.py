import requests
import time
import json
import random

API_KEY = "6efaef3d144548249db73513261006"

CITIES = [
    "Cairo",
    "Alexandria",
    "Giza",
    "Luxor",
    "Aswan",
    "Fayoum",
    "Hurghada"
]

url = "https://api.weatherapi.com/v1/current.json"

while True:
    try:
        city = random.choice(CITIES)

        response = requests.get(
            url,
            params={
                "key": API_KEY,
                "q": city
            },
            timeout=10
        )

        data = response.json()

        record = {
            "city": data["location"]["name"],
            "temp_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "wind_kph": data["current"]["wind_kph"],
            "condition": data["current"]["condition"]["text"],
            "time": data["location"]["localtime"]
        }

        line = json.dumps(record, ensure_ascii=False)

        file_path = "/home/bigdata/Desktop/weather_stream.json"

        with open(file_path, "a") as f:
            f.write(line + "\n")
            f.flush()

        print(record)

        time.sleep(12)

    except Exception as e:
        print("Error:", e)
        time.sleep(20)
