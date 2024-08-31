import sqlite3
import unittest
from unittest.mock import patch
from app import create_table, fetch_current_weather  # Updated to reflect app.py

# Use in-memory SQLite database for tests
test_conn = sqlite3.connect(':memory:')
test_cursor = test_conn.cursor()

# Override the create_table function to use in-memory database
def create_table():
    test_cursor.execute('''
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
            precipitation_type TEXT,
            air_quality_index INTEGER,
            sun_angle REAL,
            ground_temperature REAL,
            weather_warnings TEXT,
            wind_chill REAL
        )
    ''')
    test_conn.commit()

class TestWeatherData(unittest.TestCase):

    def test_create_table(self):
        print("\nRunning test_create_table:")
        try:
            create_table()
            print("SUCCESS: create_table() ran without errors.")
        except Exception as e:
            print(f"FAIL: create_table() raised an exception: {e}")
            self.fail(f"create_table() raised an exception: {e}")

    @patch('app.requests.get')  # Updated to reflect app.py
    def test_fetch_current_weather(self, mock_get):
        print("\nRunning test_fetch_current_weather:")
        mock_city = {"name": "TestCity", "lat": 0, "lon": 0}

        # Mocking the API response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            'current': {
                'temp': 70.0,
                'feels_like': 68.0,
                'humidity': 50,
                'pressure': 1013,
                'wind_speed': 5,
                'wind_deg': 180,
                'weather': [{'description': 'clear sky', 'main': 'Clear'}],
                'clouds': 0,
                'visibility': 10000,
                'rain': {'1h': 0},
                'snow': {'1h': 0},
                'sunrise': 1627891234,
                'sunset': 1627941234,
                'dew_point': 55.0,
                'uvi': 5.0,
                'wind_chill': None,
            }
        }

        try:
            fetch_current_weather(mock_city)
            print("SUCCESS: fetch_current_weather() ran without errors.")
        except Exception as e:
            print(f"FAIL: fetch_current_weather() raised an exception: {e}")
            self.fail(f"fetch_current_weather() raised an exception: {e}")

if __name__ == '__main__':
    unittest.main()
