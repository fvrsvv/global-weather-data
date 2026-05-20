from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import random
import dlt
import openmeteo_requests

from airflow.sdk import dag, task


VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation", "weather_code",
    "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
    "cloud_cover", "surface_pressure", "et0_fao_evapotranspiration"
]


def fetch_batch_weather(batch: list[dict]) -> pd.DataFrame | None:
    """Один multi-location запрос"""
    if not batch:
        return None

    try:
        now = datetime.now(timezone.utc)
        
        latitudes = [loc["lat"] for loc in batch]
        longitudes = [loc["lon"] for loc in batch]

        params = {
            "latitude": latitudes,
            "longitude": longitudes,
            "start_hour": (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:00"),
            "end_hour": now.strftime("%Y-%m-%dT%H:00"),
            "hourly": VARIABLES,
            "timezone": "UTC"
        }

        client = openmeteo_requests.Client()
        responses = client.weather_api("http://localhost:8080/v1/archive", params=params)

        all_dfs = []
        for i, response in enumerate(responses):
            loc = batch[i]
            hourly = response.Hourly()

            times = pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )

            data = {"time": times}
            for j, var in enumerate(VARIABLES):
                data[var] = hourly.Variables(j).ValuesAsNumpy()

            df = pd.DataFrame(data).tail(1).reset_index(drop=True)

            df["location_id"] = loc["place_id"]
            df["city"] = loc["city"]
            df["region"] = loc["region_name"]
            df["ingested_at"] = now

            all_dfs.append(df)

        return pd.concat(all_dfs, ignore_index=True)

    except Exception as e:
        print(f"❌ Ошибка батча ({len(batch)} городов): {e}")
        return None


@task(retries=3, retry_delay=timedelta(minutes=1))
def fetch_all_weather():
    df_locations = pd.read_csv("/opt/airflow/dags/location/towns.csv")
    df_locations = df_locations[['place_id', 'city', 'region_name', 'lat', 'lon']]
    locations = df_locations.to_dict(orient="records")

    print(f"Всего городов: {len(locations)}")

    batch_size = 40                  
    results = []
    total_success = 0
    start_time = time.time()

    for i in range(0, len(locations), batch_size):
        batch = locations[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"Запрос батча {batch_num} ({len(batch)} городов)...")

        df_batch = fetch_batch_weather(batch)

        if df_batch is not None and not df_batch.empty:
            results.append(df_batch)
            total_success += len(batch)

        elapsed = time.time() - start_time
        if elapsed < 110:
            sleep_time = random.uniform(3.8, 5.2)  
            time.sleep(sleep_time)

    total_time = time.time() - start_time
    print(f"Обработка завершена за {total_time:.1f} секунд")

    if not results:
        print("Нет данных для загрузки")
        return

    final_df = pd.concat(results, ignore_index=True)

    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_russia_hourly",
        destination="filesystem",
        dataset_name="actual_data",
    )

    pipeline.run(
        final_df,
        table_name="hourly_weather",
        loader_file_format="parquet",
        table_format="iceberg",
        write_disposition="append"
    )

    print(f"✅ Загружено: {len(final_df)} записей из {total_success} городов")


@dag(
    dag_id="open_meteo_actual_hourly",
    description="Multi-location + контролируемый темп (за ~2 минуты)",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo"],
)
def open_meteo_actual_hourly_dag():
    fetch_all_weather()


open_meteo_actual_hourly_dag()