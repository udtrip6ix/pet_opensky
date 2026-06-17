-- clickhouse/init.sql
-- Выполняется один раз при первом старте контейнера ClickHouse.

CREATE DATABASE IF NOT EXISTS opensky;

CREATE TABLE IF NOT EXISTS opensky.flights
(
    icao24          String,
    callsign        String,
    origin_country  String,
    time_position   UInt32,
    last_contact    UInt32,
    longitude       Float64,
    latitude        Float64,
    baro_altitude   Float64,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(updated_at)   -- разбивка по месяцам — ускоряет запросы по дате и упрощает очистку старых данных
ORDER BY (icao24, last_contact)     -- ключ уникальности / дедупликации
SETTINGS index_granularity = 8192;


-- Пример запроса с гарантированной дедупликацией:
-- SELECT * FROM opensky.flights FINAL WHERE toDate(updated_at) = today();