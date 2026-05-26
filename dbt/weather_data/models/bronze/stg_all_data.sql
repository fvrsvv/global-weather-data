{{ config(
    materialized = 'incremental',
    unique_key = ['time', 'location_id'],    
    incremental_strategy = 'append',         
    on_schema_change = 'append_new_columns',
    partition_by = 'toDate(time)',
    alias = 'stg_all_data'
) }}

SELECT
    *
FROM s3('https://storage.yandexcloud.net/global-weather-data/actual_data/hourly_weather/data/*.parquet',
        '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID") }}',
        '{{ env_var("DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY") }}',
        'Parquet')
{% if is_incremental() %}

WHERE ingested_at > (
    SELECT COALESCE(MAX(ingested_at), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'))
    FROM {{ this }}
)

{% endif %}