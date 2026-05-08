import dlt
import openmeteo_requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


# ========================= НАСТРОЙКИ =========================
LOCATIONS = [
    {"name": "moscow", "lat": 55.7558, "lon": 37.6173}
]

VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation", "weather_code",
    "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "surface_pressure", "et0_fao_evapotranspiration"
]

DATASET_NAME = "bronze_weather_data"
# ===========================================================


def fetch_recent_weather(lat: float, lon: float) -> pd.DataFrame:
    """Получаем данные за последние 2 дня"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

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
    response = responses[0]
    hourly = response.Hourly()

    times_unix = hourly.Time()
    times = pd.to_datetime(times_unix, unit="s")

    data = {"time": times}
    
    for i, var in enumerate(VARIABLES):
        values = hourly.Variables(i).ValuesAsNumpy()
        data[var] = values

    df = pd.DataFrame(data)
    df["location"] = "moscow"
    df["ingested_at"] = datetime.utcnow()

    print(f"✅ Загружено строк: {len(df)} | Время с {df['time'].min()} по {df['time'].max()}")
    return df


def weather_resource():
    """Генератор для dlt"""
    for loc in LOCATIONS:
        df = fetch_recent_weather(loc["lat"], loc["lon"])
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
        loader_file_format="parquet"
    )
    
    print("✅ Почасовое обновление погоды успешно завершено")


# ====================== DAG =========================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 2,
}

dag = DAG(
    dag_id="open_meteo_actual_hourly",
    default_args=default_args,
    description="Ежечасная загрузка фактической погоды из Open-Meteo",
    # schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo", "dlt"],
    is_paused_upon_creation=False,
)

ingest_weather_actual = PythonOperator(
    task_id="ingest_weather_actual",
    python_callable=run_dlt_pipeline,
    dag=dag,
)