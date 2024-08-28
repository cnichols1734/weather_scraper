import requests

API_KEY = '38797a4a8ccfa191f3cc8fe4b7d6701b'
lat = 29.7604
lon = -95.3698

url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"

response = requests.get(url)
data = response.json()

if response.status_code == 200:
    print("API call successful! Here's the data:")
    print(data)
else:
    print(f"Failed with status code: {response.status_code}")
    print(f"Error message: {data}")
