# Open-Meteo Weather Analytics Pipeline
Полноценный cloud-native ELT-пайплайн для сбора, обработки и анализа погодных и климатических данных

## О проекте
Проект представляет собой полноценный облачный ELT-пайплайн, который ежедневно собирает прогнозные погодные данные из открытого API Open-Meteo (model DWD ICON), сохраняет их в объектное хранилище и трансформирует в удобные аналитические витрины.
Цель проекта — продемонстрировать навыки построения современного data stack: от сбора сырых данных из API до готовых бизнес-витрин и дашбордов.

---

**Структура проекта**
- API --- Open-Meteo
- Ingestion --- data load tool (dlt)
- Data Lake --- Yandex Object Storage (S3)
- Orchestration --- Airflow
- DWH --- Clickhouse (local)
- Transformation --- dbt
- Visualization --- Metabase
- CI/CD --- GitHub Actions

**Архитектура проекта (Medalion)**
Bronze → dlt загружает сырые Parquet в Yandex Object Storage (table format iceberg)
Silver → dbt 
Gold → dbt-модели в DWH
Incremental loads каждый день 

---

**Настройка Metabase (http://localhost:3000)**
1. Database type: ClickHouse
2. Name: ClickHouse 
3. Host: clickhouse
4. Port: 8123
5. Username: clickhouse.username (.env)
6. Password: clickhouse.password (.env)

---

https://github.com/fvrsvv/global-weather-data