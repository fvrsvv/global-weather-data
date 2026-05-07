import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime, timedelta
import dlt
from dotenv import load_dotenv

load_dotenv()

LOCATIONS = [
    {"name": "moscow", "lat": 55.7558, "lon": 37.6173}
]

VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation", "weather_code",
    "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "surface_pressure", "et0_fao_evapotranspiration"
]

START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# ==================== dlt RESOURCE ====================
def fetch_historical_chunk(lat, lon, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": VARIABLES,
        "timezone": "Europe/Moscow"
    }
    
    openmeteo = openmeteo_requests.Client()
    responses = openmeteo.weather_api(url, params=params)
    
    # Берём первый (и единственный) ответ
    response = responses[0]
    hourly = response.Hourly()
    
    data = {
        "time": pd.date_range(start=hourly.Time(), periods=len(hourly.Time()), freq="h"),
    }
    for i, var in enumerate(VARIABLES):
        data[var] = hourly.Variables(i).ValuesAsNumpy()
    
    df = pd.DataFrame(data)
    df["location"] = "moscow"
    df["ingested_at"] = datetime.utcnow()
    return df

@dlt.resource(name="weather_actual", write_disposition="merge", primary_key=["time", "location"])
def weather_backfill():
    current = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    while current <= end:
        chunk_end = min(current + timedelta(days=30), end)  # по 30 дней — безопасно
        
        print(f"Загружаем {current.strftime('%Y-%m-%d')} — {chunk_end.strftime('%Y-%m-%d')}")
        
        for loc in LOCATIONS:
            df = fetch_historical_chunk(loc["lat"], loc["lon"], 
                                      current.strftime("%Y-%m-%d"), 
                                      chunk_end.strftime("%Y-%m-%d"))
            yield df
        
        current = chunk_end + timedelta(days=1)


# ==================== PIPELINE ====================
if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_actual_backfill",
        destination="filesystem",
        dataset_name="bronze_weather_data",
        credentials={
            "aws_access_key_id": dlt.secrets.value,
            "aws_secret_access_key": dlt.secrets.value,
            "endpoint_url": "https://storage.yandexcloud.net/bronze-weather-data",
            "region_name": "ru-central1"
        }
    )

    load_info = pipeline.run(
        weather_backfill(),
        loader_file_format="parquet",
        table_format="iceberg",
        table_name="hourly_weather",
        write_disposition="append"
    )

    print("✅ Backfill завершён, данные успешно загружены.")
    print(load_info)