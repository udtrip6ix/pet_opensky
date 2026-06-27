# pet_nasa 

Пет-проект по построению пайплайна для загрузки и анализа данных об астероидах из NASA NeoWs API.

## Архитектура

```
NASA API → Airflow DAG 1 → MinIO (Parquet) → Airflow DAG 2 → Spark → ClickHouse → Metabase
```

**Стек:**
- **Airflow** (CeleryExecutor) — оркестрация пайплайна
- **MinIO** — S3-совместимое хранилище сырых данных в формате Parquet
- **Apache Spark** — трансформация и загрузка данных
- **ClickHouse** — аналитическое хранилище
- **Metabase** — визуализация и дашборды
- **PostgreSQL** — метабаза Airflow и Metabase
- **Redis** — брокер для Celery

---

## Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone <repo_url>
cd pet_nasa
```

### 2. Заполнить `.env`

Скопируй шаблон и заполни значения:

```bash
cp .env.example .env
```

Минимальный `.env` для запуска:

```env
# Airflow
# Генерация: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_UID=1000
AIRFLOW_PROJ_DIR=.
AIRFLOW_FERNET_KEY=your-fernet-key-here
AIRFLOW_ADMIN_USERNAME=airflow
AIRFLOW_ADMIN_PASSWORD=airflow

# PostgreSQL
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow
AIRFLOW_POSTGRES_DB=airflow

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

# ClickHouse
CH_HOST=clickhouse
CH_PORT=8123
CH_DATABASE=nasa
CH_USER=default
CH_PASSWORD=

# NASA
NASA_API_KEY=your-nasa-api-key  # получить на api.nasa.gov
```

> `AIRFLOW_UID` — узнать свой: `id -u`
> Fernet key генерируется командой из комментария выше

### 3. Первый запуск (сборка образа)

```bash
docker compose up --build -d
```

Сборка занимает несколько минут — скачиваются Spark и JAR-файлы.

### 4. Настроить Airflow Variables и Connections

После запуска открой Airflow UI: **http://localhost:8080**

#### Variables
**Admin → Variables → + (Add)**

| Key          | Value                   |
|--------------|-------------------------|
| NASA_API_KEY | ключ с api.nasa.gov     |
| MINIO_BUCKET | raw-data                |
| KEY_PREFIX   | asteroids               |
| CH_TABLE     | nasa.asteroids          |
| CH_MIN_ROWS  | 1                       |
| access_key   | minioadmin              |
| secret_key   | minioadmin              |

#### Connections
**Admin → Connections → + (Add)**

**minio_s3**

| Поле                  | Значение                                                            |
|-----------------------|---------------------------------------------------------------------|
| Connection Id         | minio_s3                                                            |
| Connection Type       | Amazon Web Services                                                 |
| AWS Access Key ID     | minioadmin                                                          |
| AWS Secret Access Key | minioadmin                                                          |
| Extra                 | `{"endpoint_url": "http://minio:9000", "bucket_name": "raw-data"}` |

**clickhouse_default**

| Поле            | Значение           |
|-----------------|--------------------|
| Connection Id   | clickhouse_default |
| Connection Type | Generic            |
| Host            | clickhouse         |
| Port            | 8123               |
| Login           | default            |
| Password        | (пусто)            |
| Schema          | nasa               |

**spark_default**

| Поле            | Значение      |
|-----------------|---------------|
| Connection Id   | spark_default |
| Connection Type | Spark         |
| Host            | local         |
| Deploy mode     | client        |
| Spark binary    | spark-submit  |

### 5. Включить DAG-и

В Airflow UI включи оба DAG-а:
- `dag_nasa_to_s3` — загрузка из NASA API в MinIO
- `dag_s3_to_ch` — загрузка из MinIO в ClickHouse через Spark

---

## Управление стеком

```bash
# Запуск (после первой сборки)
docker compose up -d

# Остановка (данные сохраняются)
docker compose down

# Полный сброс (удаляет все данные)
docker compose down -v

# Логи конкретного сервиса
docker compose logs -f airflow-scheduler
```

---

## Сервисы

| Сервис       | URL                          | Логин        |
|--------------|------------------------------|--------------|
| Airflow UI   | http://localhost:8080        | airflow / airflow |
| MinIO Console| http://localhost:9001        | minioadmin / minioadmin |
| ClickHouse   | http://localhost:8123/play   | default      |
| Metabase     | http://localhost:3000        | настраивается при первом входе |