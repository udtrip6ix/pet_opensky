-- Создаем базу, если ее нет
CREATE DATABASE metabase;
CREATE DATABASE airflow;

-- Делаем пользователя airflow владельцем этих баз
ALTER DATABASE metabase OWNER TO airflow;
ALTER DATABASE airflow OWNER TO airflow;

-- Подключаемся к базе metabase, чтобы дать полные права на схему public
\c metabase
GRANT ALL ON SCHEMA public TO airflow;

-- Подключаемся к базе airflow, чтобы дать полные права на схему public
\c airflow
GRANT ALL ON SCHEMA public TO airflow;