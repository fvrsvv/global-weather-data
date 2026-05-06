import duckdb
import os
con = duckdb.connect("global_weather.duckdb")

con.execute(f"""
    SET s3_access_key_id = '"'"'{os.environ["S3_ACCESS_KEY_ID"]}'"'"';
    SET s3_secret_access_key = '"'"'{os.environ["S3_SECRET_ACCESS_KEY"]}'"'"';
    SET s3_endpoint = '"'"'storage.yandexcloud.net'"'"';
    SET s3_use_ssl = true;
    SET s3_url_style = '"'"'path'"'"';
""")

# Показать список файлов
files = con.execute("SELECT * FROM glob('s3://global-weather-data/bronze/hourly_weather/data/*')").df()
print(files)
