{{ config(
    materialized = 'table',
    alias = 'stg_hourly_weather'
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
FROM s3('https://storage.yandexcloud.net/global-weather-data/bronze/hourly_weather/data/*.parquet')