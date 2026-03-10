from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from bs4 import BeautifulSoup
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_amazon_books(**kwargs):
    url = "https://books.toscrape.com"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')
    books = []

    for item in soup.select('article.product_pod'):
        title = item.select_one('h3 a')['title']
        price = item.select_one('.price_color').get_text(strip=True).replace('£', '')
        rating_word = item.select_one('p.star-rating')['class'][1]
        rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

        books.append({
            'title': title,
            'author': None,
            'price': float(price),
            'rating': rating_map.get(rating_word),
        })

    kwargs['ti'].xcom_push(key='raw_books', value=books)
    print(f"Fetched {len(books)} books")


def deduplicate_books(**kwargs):
    books = kwargs['ti'].xcom_pull(key='raw_books', task_ids='fetch_amazon_books')
    df = pd.DataFrame(books)
    df.drop_duplicates(subset='title', inplace=True)
    df.reset_index(drop=True, inplace=True)
    kwargs['ti'].xcom_push(key='clean_books', value=df.to_dict(orient='records'))
    print(f"{len(df)} books after deduplication")


def load_to_postgres(**kwargs):
    books = kwargs['ti'].xcom_pull(key='clean_books', task_ids='deduplicate_books')
    df = pd.DataFrame(books)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS amazon_books (
            id SERIAL PRIMARY KEY,
            title VARCHAR(500),
            author VARCHAR(255),
            price NUMERIC(10, 2),
            rating NUMERIC(3, 1),
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    rows = list(df.itertuples(index=False, name=None))
    hook.insert_rows(
        table='amazon_books',
        rows=rows,
        target_fields=['title', 'author', 'price', 'rating'],
    )
    print(f"Inserted {len(rows)} rows into amazon_books")


with DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='Fetch Amazon book data and store in Postgres',
    schedule=timedelta(days=1),
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_amazon_books',
        python_callable=fetch_amazon_books,
    )

    deduplicate_task = PythonOperator(
        task_id='deduplicate_books',
        python_callable=deduplicate_books,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    fetch_task >> deduplicate_task >> load_task
