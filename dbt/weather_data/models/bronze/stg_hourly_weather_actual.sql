{{ config(
    materialized = 'table',
    alias = 'stg_hourly_weather_actual'
) }}

SELECT
    location_name,
    temperature_2m               as temp_c,
    relative_humidity_2m         as humidity_percent,
    apparent_temperature         as feels_like_c,
    precipitation,
    wind_speed_10m               as wind_speed_kmh,
    wind_direction_10m           as wind_direction,
    cloud_cover,
    weather_code,
    surface_pressure, 
    et0_fao_evapotranspiration
    load_date
FROM s3('https://storage.yandexcloud.net/global-weather-data/hourly_weather_data/hourly_weather/*.parquet',
        '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID") }}',
        '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY") }}',
        'Parquet')