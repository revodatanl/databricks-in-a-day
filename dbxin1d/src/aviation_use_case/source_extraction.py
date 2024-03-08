import json
import requests
from datetime import datetime, timedelta

header = {
    "authority": "be.wizzair.com",
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en;q=0.9",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://wizzair.com",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}

payload = {
    "flightList": [
        {
            "departureStation": "EIN",
            "arrivalStation": "BUD",
            "from": "INSERT",
            "to": "INSERT",
        },
        {
            "departureStation": "BUD",
            "arrivalStation": "EIN",
            "from": "INSERT",
            "to": "INSERT",
        },
    ],
    "priceType": "wdc",
    "adultCount": 1,
}

# Setting the parameters
LENGHT = 40  # in days
now = datetime.now()
today_formatted = now.strftime("%Y-%m-%d")
extra_days = now + timedelta(days=LENGHT)
extra_days_formatted = extra_days.strftime("%Y-%m-%d")

# Inserting dynamic data values into configuration
payload["flightList"][0]["from"] = today_formatted
payload["flightList"][1]["from"] = today_formatted
payload["flightList"][0]["to"] = extra_days_formatted
payload["flightList"][1]["to"] = extra_days_formatted

# Hitting the API endpoint
response = requests.post(
    "https://be.wizzair.com/20.7.0/Api/search/timetable", headers=header, json=payload
)

# Handling the response from the server
if response.raise_for_status():
    print("Something went wront with the request.")
    print(response.status_code)
else:
    data = json.loads(response.content)
    dbutils.fs.put(
        f"dbfs:/Volumes/databricks_training/raw/aviation/{today_formatted}_prices_EIN_BUD.json",
        json.dumps(data),
        True,
    )