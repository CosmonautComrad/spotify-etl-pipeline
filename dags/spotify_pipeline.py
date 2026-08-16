from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import csv
import os

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
PATH = os.path.join(AIRFLOW_HOME, 'dags/data/')
FILE_NAME = 'most_streamed_spotify_2025.csv'
TOP_THRESHOLD = 830599247

# 1. Настраиваем сам DAG через декоратор
@dag(
    dag_id='spotify_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1), 
    schedule=None, 
    catchup=False, 
    tags=['learning', 'spotify']
)
def spotify_pipeline():

    # 2. Первая таска — экстракт данных
    @task()
    def extract_data():
        file_path = os.path.join(PATH, FILE_NAME)
        raw_data = []

        with open (file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_data.append(row)

        return raw_data

    # 3. Вторая таска — трансформация
    @task()
    def transform_data(raw_data: list):
        transformed_data = []
        keys = ['track', 'artist', 'spotify_streams_total']
        for item in raw_data:
            tmp = {key: item[key] for key in keys if key in item}
            tmp['is_hit'] = bool(int(tmp['spotify_streams_total']) > TOP_THRESHOLD)
            transformed_data.append(tmp)        
        
        return transformed_data

    # 4. Третья таска — проверка качества данных       
    @task()
    def check_data_quality(transformed_data: list):
        if len(transformed_data) < 1:
            raise ValueError("Data quality check failed: Task received and empty list")
        else:
            print(f"Data quality check passed. Rows number: {len(transformed_data)}")

    # 5. Четвёртая таска — загрузка в БД
    @task()
    def load_data(transformed_data: list):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
            INSERT INTO spotify_top_songs (track_name, artist_name, streams, is_mega_hit)
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (track_name, artist_name)
            DO UPDATE SET
                streams = EXCLUDED.streams,
                is_mega_hit = EXCLUDED.is_mega_hit,
                loaded_at = CURRENT_TIMESTAMP ;
        """

        for item in transformed_data:
            keys = ('track', 'artist', 'spotify_streams_total', 'is_hit')
            pg_hook.run(sql, parameters=tuple(item[k] for k in keys))

        print(f"Успешно загружено {len(transformed_data)} треков в БД!")

    # 6. Пятая таска — вывести топ-3 артистов
    @task()
    def calculate_top_3_artists():
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
            SELECT 
                artist_name,
                SUM(streams) as total_streams
            FROM 
                spotify_top_songs
            GROUP BY 
                artist_name
            ORDER BY 
                total_streams DESC
            LIMIT 3 ;
        """

        result = pg_hook.get_records(sql)

        print("---Топ-3 артиста по стримам ---")
        for row in result:
            print(f"Артист: {row[0]} | Всего стримов: {row[1]}")


    # 7. Связываем таски между собой (TaskFlow API)
    raw_data = extract_data()
    processed_data = transform_data(raw_data)

    quality_check = check_data_quality(processed_data)

    load_task = load_data(processed_data)

    quality_check >> load_task >> calculate_top_3_artists()

# Инициализируем DAG
spotify_pipeline()
