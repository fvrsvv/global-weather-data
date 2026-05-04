{{ config(
    materialized = 'view',
    alias = 'hourly_weather'
) }}

SELECT 
    "timestamp"                  as weather_timestamp,
    city,
    temperature_2m               as temp_c,
    relative_humidity_2m         as humidity_percent,
    apparent_temperature         as feels_like_c,
    precipitation,
    rain,
    showers,
    snowfall,
    wind_speed_10m               as wind_speed_kmh,
    wind_direction_10m           as wind_direction,
    cloud_cover,
    pressure_msl                 as pressure_hpa,
    weather_code,
    ingestion_time,
    load_date
FROM read_parquet(
    's3://global-weather-data/bronze/hourly_weather/*.parquet',
    s3_access_key_id     = '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID") }}',
    s3_secret_access_key = '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY") }}',
    s3_endpoint          = 'storage.yandexcloud.net',
    s3_url_style = 'path'
)