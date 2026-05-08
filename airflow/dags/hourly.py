import dlt
import openmeteo_requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


LOCATIONS = [
    {"name": "moscow", "lat": 55.7558, "lon": 37.6173}
]

VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation", "weather_code",
    "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "surface_pressure", "et0_fao_evapotranspiration"
]

DATASET_NAME = "bronze_weather_data"

def fetch_recent_weather(lat: float, lon: float, location_name: str) -> pd.DataFrame:
    """Получаем данные только за последний час"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    now = datetime.utcnow()

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_hour": (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:00"),
        "end_hour": now.strftime("%Y-%m-%dT%H:00"),
        "hourly": VARIABLES,
        "timezone": "UTC"                    
    }

    openmeteo = openmeteo_requests.Client()
    responses = openmeteo.weather_api(url, params=params)
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

    df["location_name"] = location_name
    df["ingested_at"] = datetime.utcnow()
    return df


def weather_resource():
    """Генератор для dlt"""
    for loc in LOCATIONS:
        df = fetch_recent_weather(loc["lat"], loc["lon"], loc["name"])
        yield df

def run_dlt_pipeline():
    """Основная функция"""
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_actual_hourly",
        destination="filesystem",
        dataset_name=DATASET_NAME,
    )

    pipeline.run(
        weather_resource(),
        loader_file_format="parquet",
        table_name="hourly_weather"
    )
    
    print("✅ Hourly weather update completed successfully!")


# ====================== DAG =========================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
}

dag = DAG(
    dag_id="open_meteo_actual_hourly",
    default_args=default_args,
    description="Ежечасная загрузка фактической погоды (только последний час)",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo", "dlt"],
    is_paused_upon_creation=False,
)

ingest_weather_actual = PythonOperator(
    task_id="ingest_weather_actual",
    python_callable=run_dlt_pipeline,
    dag=dag,
)