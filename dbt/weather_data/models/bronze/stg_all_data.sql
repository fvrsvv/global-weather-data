{{ config(
    materialized = 'table',
    alias = 'stg_all_data'
) }}

SELECT
*
FROM s3('https://storage.yandexcloud.net/global-weather-data/actual_data/hourly_weather/data/*.parquet',
        '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID") }}',
        '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY") }}',
        'Parquet')