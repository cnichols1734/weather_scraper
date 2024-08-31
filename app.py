import requests
import sqlite3
import time
from datetime import datetime

# API key
API_KEY = '38797a4a8ccfa191f3cc8fe4b7d6701b'

# List of cities
cities = [
    {"name": "Houston", "lat": 29.7604, "lon": -95.3698},
    {"name": "Dallas", "lat": 32.7767, "lon": -96.7970},
    {"name": "Austin", "lat": 30.2672, "lon": -97.7431},
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"name": "Denver", "lat": 39.7392, "lon": -104.9903}
]

# SQLite database connection
conn = sqlite3.connect('weather_data_updated.db')
cursor = conn.cursor()

# Function to create the table
def create_table():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name TEXT,
            timestamp DATETIME,
            temperature REAL,
            feels_like REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            wind_direction REAL,
            weather_description TEXT,
            cloudiness INTEGER,
            visibility INTEGER,
            rain_volume REAL,
            snow_volume REAL,
            sunrise DATETIME,
            sunset DATETIME,
            dew_point REAL,
            uv_index REAL,
            precipitation_type TEXT
        )
    ''')
    conn.commit()

# Function to fetch current weather data using the One Call API
def fetch_current_weather(city):
    lat = city["lat"]
    lon = city["lon"]
    url = f"http://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={API_KEY}&units=imperial"

    response = requests.get(url)
    data = response.json()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if response.status_code == 200:
        # Parsing the current weather data
        weather_info = {
            "city_name": city["name"],
            "timestamp": datetime.now(),
            "temperature": data['current']['temp'],
            "feels_like": data['current']['feels_like'],
            "humidity": data['current']['humidity'],
            "pressure": data['current']['pressure'],
            "wind_speed": data['current']['wind_speed'],
            "wind_direction": data['current'].get('wind_deg', None),
            "weather_description": data['current']['weather'][0]['description'],
            "cloudiness": data['current']['clouds'],
            "visibility": data.get('current', {}).get('visibility', None) / 3.281 if data.get('current', {}).get('visibility') else None,  # Convert meters to feet
            "rain_volume": data.get('current', {}).get('rain', {}).get('1h', 0),
            "snow_volume": data.get('current', {}).get('snow', {}).get('1h', 0),
            "sunrise": datetime.fromtimestamp(data['current']['sunrise']),
            "sunset": datetime.fromtimestamp(data['current']['sunset']),
            "dew_point": data['current']['dew_point'],
            "uv_index": data['current']['uvi'],
            "precipitation_type": data['current']['weather'][0]['main']
        }

        # Insert data into SQLite database
        cursor.execute('''
            INSERT INTO weather_data (
                city_name, timestamp, temperature, feels_like, humidity, pressure,
                wind_speed, wind_direction, weather_description, cloudiness,
                visibility, rain_volume, snow_volume, sunrise, sunset,
                dew_point, uv_index, precipitation_type
            ) VALUES (
                :city_name, :timestamp, :temperature, :feels_like, :humidity, :pressure,
                :wind_speed, :wind_direction, :weather_description, :cloudiness,
                :visibility, :rain_volume, :snow_volume, :sunrise, :sunset,
                :dew_point, :uv_index, :precipitation_type
            )
        ''', weather_info)

        conn.commit()

        print(f"[{current_time}] SUCCESS: Weather data for {city['name']} inserted successfully!")
        print(f"    - Temperature: {weather_info['temperature']}°F, Feels Like: {weather_info['feels_like']}°F")
        print(f"    - Humidity: {weather_info['humidity']}%, Pressure: {weather_info['pressure']} hPa")
        print(f"    - Wind: {weather_info['wind_speed']} mph from {weather_info['wind_direction']}°")
        print(f"    - Description: {weather_info['weather_description']}, Cloudiness: {weather_info['cloudiness']}%")
        print(f"    - Visibility: {weather_info['visibility']} feet, Rain: {weather_info['rain_volume']} inches, Snow: {weather_info['snow_volume']} inches")
        print(f"    - Sunrise: {weather_info['sunrise'].strftime('%Y-%m-%d %H:%M:%S')}, Sunset: {weather_info['sunset'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"    - Dew Point: {weather_info['dew_point']}°F, UV Index: {weather_info['uv_index']}, Precipitation Type: {weather_info['precipitation_type']}")
    else:
        print(f"[{current_time}] ERROR: Failed to get weather data for {city['name']}. HTTP Status code: {response.status_code}")
        print(f"    - Response Content: {data}\n")

# Run this once to create the table
create_table()

# Main loop to fetch data every 15 minutes
try:
    while True:
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"---\n[{start_time}] STARTING WEATHER DATA COLLECTION FOR ALL CITIES\n")
        for city in cities:
            fetch_current_weather(city)
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{end_time}] COMPLETED WEATHER DATA COLLECTION. Sleeping for 10 minutes...\n---\n")
        time.sleep(600)  # Sleep for 10 minutes
except KeyboardInterrupt:
    print("Script interrupted by user. Closing the database connection.")
finally:
    conn.close()
