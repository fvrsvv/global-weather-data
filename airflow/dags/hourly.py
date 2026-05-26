from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import dlt
import openmeteo_requests

from airflow.sdk import dag, task


VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "weather_code",
    "wind_u_component_10m",
    "wind_v_component_10m",
    "wind_gusts_10m",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high"
]

def fetch_batch_weather(batch: list[dict], days_back: int = 7) -> pd.DataFrame | None:
    """Fetch historical weather data for a batch of locations"""
    if not batch:
        return None

    try:
        now = datetime.now(timezone.utc)
        start_date = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")

        latitudes = [loc["lat"] for loc in batch]
        longitudes = [loc["lon"] for loc in batch]

        params = {
            "latitude": latitudes,
            "longitude": longitudes,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(VARIABLES),
            "timezone": "UTC",
            "models": "dwd_icon"          
        }

        client = openmeteo_requests.Client()
        responses = client.weather_api("http://172.19.0.1:8085/v1/archive", params=params)

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

            df = pd.DataFrame(data)

            df["location_id"] = loc["place_id"]
            df["city"] = loc["city"]
            df["region"] = loc["region_name"]
            df["ingested_at"] = now

            all_dfs.append(df)

        return pd.concat(all_dfs, ignore_index=True)

    except Exception as e:
        print(f"❌ Error batch ({len(batch)} cities): {e}")
        return None


@task(retries=3, retry_delay=timedelta(minutes=2))
def fetch_all_weather():
    df_locations = pd.read_csv("/opt/airflow/dags/location/towns.csv")
    df_locations = df_locations[['place_id', 'city', 'region_name', 'lat', 'lon']]
    locations = df_locations.to_dict(orient="records")

    print(f"Total cities: {len(locations)}")

    batch_size = 100       
    results = []
    total_success = 0
    start_time = time.time()

    DAYS_BACK = 1                   

    for i in range(0, len(locations), batch_size):
        batch = locations[i:i + batch_size]
        batch_num = i // batch_size + 1
        print(f"Fetching batch {batch_num} ({len(batch)} cities) | Period: {DAYS_BACK} days...")

        df_batch = fetch_batch_weather(batch, days_back=DAYS_BACK)

        if df_batch is not None and not df_batch.empty:
            results.append(df_batch)
            total_success += len(batch)

    total_time = time.time() - start_time
    print(f"Processing completed in {total_time:.1f} seconds")

    if not results:
        print("No data to upload")
        return

    final_df = pd.concat(results, ignore_index=True)
    print(f"Total records: {len(final_df)}")

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

    print(f"✅ Uploaded: {len(final_df)} records from {total_success} cities")


@dag(
    dag_id="open_meteo_actual_hourly",
    description="Fetch historical weather data from local Open-Meteo (DWD_ICON)",
    schedule="0 */3 * * *",           
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "open-meteo"],
)
def open_meteo_actual_hourly_dag():
    fetch_all_weather()


open_meteo_actual_hourly_dag()