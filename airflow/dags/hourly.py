from datetime import datetime, timedelta, timezone
import pandas as pd
import dlt
import openmeteo_requests

from airflow.decorators import dag, task

VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation", "weather_code",
    "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "surface_pressure", "et0_fao_evapotranspiration"
]

def fetch_recent_weather(lat: float, lon: float, city_name: str, location_id: int, region: str) -> pd.DataFrame:
    """Получаем данные только за последний час по одному городу"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    now = datetime.now(timezone.utc)

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_hour": (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:00"),
        "end_hour": now.strftime("%Y-%m-%dT%H:00"),
        "hourly": VARIABLES,
        "timezone": "UTC"
    }

    client = openmeteo_requests.Client()
    responses = client.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    data = {"time": times}
    for i, var in enumerate(VARIABLES):
        data[var] = hourly.Variables(i).ValuesAsNumpy()

    df = pd.DataFrame(data)
    df = df.tail(1).reset_index(drop=True)  

    df["location_id"] = location_id
    df["city"] = city_name
    df["region"] = region
    df["ingested_at"] = now

    return df

@task
def get_active_locations() -> list[dict]:
    """Загружаем список активных городов"""
    df = pd.read_csv("/opt/airflow/dags/location/towns.csv")
    df = df[['place_id', 'city', 'region_name', 'lat', 'lon']]
    return df.to_dict(orient="records")

@task
def fetch_and_load_weather(location: dict):
    """Задача на один город: забирает данные и сразу грузит в Iceberg"""
    df = fetch_recent_weather(
        lat=location["lat"],
        lon=location["lon"],
        city_name=location["city"],
        location_id=location["place_id"],
        region=location["region_name"]
    )

    if df.empty:
        print(f"Нет данных для города {location['city']}")
        return

    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_russia_hourly",
        destination="filesystem",
        dataset_name="actual_data",          
    )

    pipeline.run(df,
        table_name="hourly_weather",
        loader_file_format="parquet",
        table_format="iceberg",
        write_disposition="append"
    )

    print(f"✅ Загружено: {location['city']} ({location['region_name']})")

@dag(
    dag_id="open_meteo_actual_hourly",
    description="Ежечасная загрузка фактической погоды по всем городам России (последний час)",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo", "dlt", "russia"],
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def open_meteo_actual_hourly_dag():
    locations = get_active_locations()
    fetch_and_load_weather.expand(location=locations)

open_meteo_actual_hourly_dag()