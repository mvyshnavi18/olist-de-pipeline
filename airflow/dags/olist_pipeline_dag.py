from airflow.decorators import dag, task
from datetime import datetime
import sys
sys.path.insert(0, '/opt/airflow')

@dag(
    dag_id='olist_etl_pipeline',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['olist', 'etl', 'portfolio'],
    default_args={
        'retries': 2,
        'retry_delay': 300,
    }
)
def olist_pipeline():

    @task
    def validate_raw_files():
        import os
        required = [
            'olist_orders_dataset.csv',
            'olist_order_items_dataset.csv',
            'olist_order_payments_dataset.csv',
            'olist_order_reviews_dataset.csv',
            'olist_customers_dataset.csv',
            'olist_products_dataset.csv',
            'olist_sellers_dataset.csv',
            'olist_geolocation_dataset.csv',
            'product_category_name_translation.csv',
        ]
        base = '/opt/airflow/data/raw'
        missing = [f for f in required if not os.path.exists(f'{base}/{f}')]
        if missing:
            raise FileNotFoundError(f'Missing files: {missing}')
        import logging
        logging.getLogger(__name__).info(f'All {len(required)} files found')
        return 'ok'

    @task
    def extract_and_load():
        from src.extract_load import run_all
        total = run_all()
        return f'Loaded {total} total rows'

    @task
    def run_dbt():
        import subprocess
        result = subprocess.run(
            ['dbt', 'run',
             '--project-dir', '/opt/airflow/dbt_project',
             '--profiles-dir', '/opt/airflow/dbt_project'],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f'dbt run failed:\n{result.stderr}')
        return 'dbt run complete'

    @task
    def run_dbt_tests():
        import subprocess
        result = subprocess.run(
            ['dbt', 'test',
             '--project-dir', '/opt/airflow/dbt_project',
             '--profiles-dir', '/opt/airflow/dbt_project'],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(f'dbt test failed:\n{result.stderr}')
        return 'dbt tests passed'

    v = validate_raw_files()
    e = extract_and_load()
    d = run_dbt()
    t = run_dbt_tests()

    v >> e >> d >> t

olist_pipeline()
