import requests
import sqlite3
import time
from datetime import datetime
import json
import logging

# Set up logging
logging.basicConfig(filename='weather_data.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load API key from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

API_KEY = config.get('API_KEY')

if not API_KEY:
    raise ValueError("No API key found. Make sure to add your API key to config.json.")

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

# Custom adapter to convert datetime to string
def adapt_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# Custom converter to convert string back to datetime
def convert_datetime(s):
    return datetime.strptime(s.decode('utf-8'), '%Y-%m-%d %H:%M:%S')

# Register the adapter and converter with SQLite
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)

# SQLite database connection (with `detect_types` for custom conversion)
conn = sqlite3.connect('weather_data_updated.db', detect_types=sqlite3.PARSE_DECLTYPES)
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

    for attempt in range(3):  # Try up to 3 times
        try:
            response = requests.get(url)
            data = response.json()
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if response.status_code == 200:
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
                return weather_info
            else:
                logging.error(f"Failed to get weather data for {city['name']}. HTTP Status code: {response.status_code}")
                logging.error(f"Response Content: {data}")

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching weather data for {city['name']}: {e}")
            time.sleep(5)  # Wait before retrying

    return None  # Return None if all attempts fail

# Run this once to create the table
create_table()

# Main loop to fetch data every 15 minutes
try:
    while True:
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"---\n[{start_time}] STARTING WEATHER DATA COLLECTION FOR ALL CITIES\n")
        batch_data = []

        for city in cities:
            weather_data = fetch_current_weather(city)
            if weather_data:
                batch_data.append(weather_data)
                print(f"SUCCESS: Weather data for {city['name']} collected.")

        if batch_data:
            cursor.executemany('''
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
            ''', batch_data)
            conn.commit()

            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{end_time}] COMPLETED WEATHER DATA COLLECTION. Inserted {len(batch_data)} records.\n")
        else:
            print("WARNING: No data collected this cycle.\n")

        time_taken = time.time() - datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S").timestamp()
        print(f"Sleeping for {max(0, 900 - time_taken)} seconds...\n---\n")
        time.sleep(max(0, 900 - time_taken))  # Adjust sleep to account for the time taken by the operation

except KeyboardInterrupt:
    print("Script interrupted by user. Closing the database connection.")
finally:
    conn.close()
