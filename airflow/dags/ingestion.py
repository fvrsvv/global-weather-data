import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime
import dlt
import os
from dotenv import load_dotenv

load_dotenv()

CITIES = [
    {"name": "Moscow", "latitude": 55.7558, "longitude": 37.6173},
    {"name": "Saint Petersburg", "latitude": 59.9343, "longitude": 30.3351},
    {"name": "Novosibirsk", "latitude": 55.0084, "longitude": 82.9357},
    {"name": "Yekaterinburg", "latitude": 56.8389, "longitude": 60.6057},
    {"name": "Berlin", "latitude": 52.5200, "longitude": 13.4049},
    {"name": "London", "latitude": 51.5085, "longitude": -0.1257},
    {"name": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "Tokyo", "latitude": 35.6895, "longitude": 139.6917},
]

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "snowfall",
    "showers",
    "rain",
    "precipitation",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "surface_pressure"
]

# ==================== dlt RESOURCE ====================
@dlt.resource(name="hourly_weather", write_disposition="append")
def open_meteo_weather():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    latitudes = [city["latitude"] for city in CITIES]
    longitudes = [city["longitude"] for city in CITIES]
    city_names = [city["name"] for city in CITIES]

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "hourly": HOURLY_VARIABLES,
        "models": "gfs_seamless",
        "timezone": "auto",
        "forecast_days": 7
    }

    responses = openmeteo.weather_api(url, params=params)

    for idx, response in enumerate(responses):
        city_name = city_names[idx]
        print(f"✅ Обработан: {city_name}")

        hourly = response.Hourly()
        time = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        hourly_data = {"timestamp": time, "city": city_name}
        for i, var_name in enumerate(HOURLY_VARIABLES):
            hourly_data[var_name] = hourly.Variables(i).ValuesAsNumpy()

        df = pd.DataFrame(data=hourly_data)
        df["ingestion_time"] = datetime.utcnow()
        df["source"] = "open-meteo"
        df["model"] = "gfs_seamless"
        df["load_date"] = datetime.utcnow().date().isoformat()

        yield df


# ==================== PIPELINE ====================
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="global_weather",
        destination="filesystem",
        dataset_name="bronze",
    )

    load_info = pipeline.run(
        open_meteo_weather(),
        loader_file_format="parquet",
        table_format="iceberg",
        table_name="hourly_weather",
        write_disposition="append"
    )

    print("✅ Данные успешно загружены!")
    print(load_info)