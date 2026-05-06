# Open-Meteo Weather Analytics Pipeline
Полноценный cloud-native ELT-пайплайн для сбора, обработки и анализа погодных и климатических данных

## О проекте
Проект представляет собой полноценный облачный ELT-пайплайн, который ежедневно собирает исторические и прогнозные погодные данные из открытого API Open-Meteo, сохраняет их в объектное хранилище и трансформирует в удобные аналитические витрины.
Цель проекта — продемонстрировать навыки построения современного data stack: от ingestion сырых данных из API до готовых бизнес-витрин и дашбордов.

---

**Структура проекта**
- API --- Open-Meteo
- Ingestiondlt --- data load tool (dlt)
- Data Lake --- Yandex Object Storage (S3)
- Orchestration --- Airflow
- Warehouse --- MotherDuck/BigQuery/Snowflake/Databricks SQL
- Transformation --- dbt-core + dbt-duckd
- Visualization --- Streamlit (или Metabase)
- CI/CD --- GitHub Actions

**Архитектура проекта (Medalion)**
Bronze → dlt загружает сырые Parquet в Yandex Object Storage 
Silver → dbt или Python task в Airflow 
Gold → dbt-модели в DWH (аналитические витрины)
Incremental loads каждый день 

https://github.com/fvrsvv/global-weather-data