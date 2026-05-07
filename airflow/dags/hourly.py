import dlt
import openmeteo_requests
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


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


def fetch_recent_weather(lat, lon):
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
def weather_resource():
    """dlt resource"""
    for loc in LOCATIONS:
        df = fetch_recent_weather(loc["lat"], loc["lon"])
        yield df


def run_dlt_pipeline():
    """Основная функция, которую будет вызывать Airflow"""
    
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_actual_hourly",
        destination="filesystem",
        dataset_name=DATASET_NAME,
        credentials={
            "aws_access_key_id": Variable.get("YANDEX_ACCESS_KEY_ID"),
            "aws_secret_access_key": Variable.get("YANDEX_SECRET_ACCESS_KEY"),
            "endpoint_url": "https://storage.yandexcloud.net",
            "region_name": "ru-central1"
        }
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="open_meteo_actual_hourly",
    default_args=default_args,
    description="Ежечасная загрузка фактической погоды из Open-Meteo",
    schedule_interval="0 * * * *",      # каждый час в :00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo", "dlt"],
) as dag:

    hourly_task = PythonOperator(
        task_id="ingest_weather_actual",
        python_callable=run_dlt_pipeline,
        provide_context=True,
    )

    hourly_task