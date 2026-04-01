"""
Bronze Layer: Ingest raw weather data from OpenWeather API
"""

# Note:
# In Databricks, configuration is imported using:
# %run ../includes/configuration

import requests
import json
from datetime import datetime

# Retrieve API key securely from Databricks Secrets (Azure Key Vault backed)
API_key = dbutils.secrets.get(scope="OpenWeather-scope", key="API-key")

# -----------------------------
# Step 1: Get coordinates
# -----------------------------
city_name = "Eindhoven"
country_code = "NL"

geo_url = (
    f"http://api.openweathermap.org/geo/1.0/direct"
    f"?q={city_name},{country_code}&appid={API_key}"
)

geo_response = requests.get(geo_url)

if geo_response.status_code != 200:
    raise Exception("Failed to fetch geolocation data")

geo = geo_response.json()

if not geo:
    raise Exception("No location data returned from API")

lat = geo[0]["lat"]
lon = geo[0]["lon"]

# -----------------------------
# Step 2: Fetch weather data
# -----------------------------
weather_url = (
    f"https://api.openweathermap.org/data/2.5/weather"
    f"?lat={lat}&lon={lon}&units=metric&appid={API_key}"
)

weather_response = requests.get(weather_url)

if weather_response.status_code != 200:
    raise Exception("Failed to fetch weather data")

weather = weather_response.json()

# -----------------------------
# Step 3: Save raw data
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

file_path = f"{raw_folder_path}/weather_{city_name}_{timestamp}.json"

dbutils.fs.put(file_path, json.dumps(weather), overwrite=True)

# -----------------------------
# Step 4: Update control file
# -----------------------------
control_file_path = f"{raw_folder_path}/latest_weather.txt"

dbutils.fs.put(control_file_path, file_path, overwrite=True)