import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CITIES = [
    {"name": "New York",    "lat": 40.7128,  "lon": -74.0060},
    {"name": "London",      "lat": 51.5074,  "lon": -0.1278},
    {"name": "Tokyo",       "lat": 35.6762,  "lon": 139.6503},
    {"name": "Paris",       "lat": 48.8566,  "lon": 2.3522},
    {"name": "Dubai",       "lat": 25.2048,  "lon": 55.2708},
    {"name": "Sydney",      "lat": -33.8688, "lon": 151.2093},
    {"name": "Toronto",     "lat": 43.6532,  "lon": -79.3832},
    {"name": "Singapore",   "lat": 1.3521,   "lon": 103.8198},
    {"name": "Berlin",      "lat": 52.5200,  "lon": 13.4050},
    {"name": "Mumbai",      "lat": 19.0760,  "lon": 72.8777},
    {"name": "Los Angeles", "lat": 34.0522,  "lon": -118.2437},
    {"name": "Chicago",     "lat": 41.8781,  "lon": -87.6298},
    {"name": "Amsterdam",   "lat": 52.3676,  "lon": 4.9041},
    {"name": "Seoul",       "lat": 37.5665,  "lon": 126.9780},
    {"name": "Barcelona",   "lat": 41.3851,  "lon": 2.1734}
]

def get_db_engine():
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)

def create_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                temperature FLOAT,
                feels_like FLOAT,
                humidity INTEGER,
                pressure INTEGER,
                wind_speed FLOAT,
                weather_condition VARCHAR(100),
                timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        conn.commit()
    print("Table ready!")

def fetch_weather(city):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current": [
            "temperature_2m",
            "apparent_temperature",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
            "weather_code"
        ],
        "timezone": "auto"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def get_weather_condition(code):
    if code == 0:
        return "Clear"
    elif code in [1, 2, 3]:
        return "Cloudy"
    elif code in [45, 48]:
        return "Foggy"
    elif code in [51, 53, 55, 61, 63, 65]:
        return "Rainy"
    elif code in [71, 73, 75, 77]:
        return "Snowy"
    elif code in [80, 81, 82]:
        return "Showers"
    elif code in [95, 96, 99]:
        return "Thunderstorm"
    else:
        return "Unknown"

def parse_weather(data, city):
    current = data["current"]
    return {
        "city": city["name"],
        "temperature": current["temperature_2m"],
        "feels_like": current["apparent_temperature"],
        "humidity": current["relative_humidity_2m"],
        "pressure": current["surface_pressure"],
        "wind_speed": current["wind_speed_10m"],
        "weather_condition": get_weather_condition(current["weather_code"]),
        "timestamp": datetime.utcnow()
    }

def load_to_db(records, engine):
    df = pd.DataFrame(records)
    df.to_sql("weather_data", engine, if_exists="append", index=False)
    print(f"Loaded {len(df)} records to database")

def run_pipeline():
    print(f"\nRunning pipeline at {datetime.now()}")
    engine = get_db_engine()
    create_table(engine)

    records = []
    for city in CITIES:
        try:
            data = fetch_weather(city)
            record = parse_weather(data, city)
            records.append(record)
            print(f"Fetched: {city['name']} — {record['temperature']}°C, {record['weather_condition']}")
        except Exception as e:
            print(f"Error fetching {city['name']}: {e}")

    if records:
        load_to_db(records, engine)
        print(f"Pipeline completed — {len(records)}/{len(CITIES)} cities fetched")
    else:
        print("No records fetched")

if __name__ == "__main__":
    run_pipeline()
