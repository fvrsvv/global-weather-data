import openmeteo_requests
import pandas as pd
from datetime import datetime, timedelta
import dlt
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

load_dotenv()

LOCATIONS = [
    {"name": "moscow", "lat": 55.7558, "lon": 37.6173}
]

VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation", "weather_code",
    "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "surface_pressure", "et0_fao_evapotranspiration"
]

START_DATE = "2026-01-01"
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
    response = responses[0]
    hourly = response.Hourly()
    
    num_values = hourly.Variables(0).ValuesLength()
    interval_sec = hourly.Interval()
    
    time_index = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        periods=num_values,
        freq=pd.Timedelta(seconds=interval_sec)
    )
    
    data = {
        "time": time_index
    }
    
    for i, var in enumerate(VARIABLES):
        data[var] = hourly.Variables(i).ValuesAsNumpy()
    
    df = pd.DataFrame(data)
    df["location"] = "moscow"
    df["ingested_at"] = datetime.now(timezone.utc)
    
    return df

@dlt.resource(name="weather_actual", write_disposition="merge", primary_key=["time", "location"])
def weather_backfill():
    current = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    while current <= end:
        chunk_end = min(current + timedelta(days=30), end)  
        
        print(f"Загружаем {current.strftime('%Y-%m-%d')} — {chunk_end.strftime('%Y-%m-%d')}")
        
        for loc in LOCATIONS:
            df = fetch_historical_chunk(loc["lat"], loc["lon"], 
                                      current.strftime("%Y-%m-%d"), 
                                      chunk_end.strftime("%Y-%m-%d"))
            yield df
        
        current = chunk_end + timedelta(days=1)


# ==================== PIPELINE ====================
def run_dlt_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_actual_backfill",
        destination="filesystem",
        dataset_name="backfill_data",
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

# ====================== DAG =========================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 3,
}

dag = DAG(
    dag_id="open_meteo_backfill_hourly",
    default_args=default_args,
    description="Ежечасная загрузка погоды c 01.01.2026",
    catchup=False,
    tags=["weather", "open-meteo", "dlt"],
    is_paused_upon_creation=False,
)

ingest_weather_actual = PythonOperator(
    task_id="ingest_weather_actual",
    python_callable=run_dlt_pipeline,
    dag=dag,
)