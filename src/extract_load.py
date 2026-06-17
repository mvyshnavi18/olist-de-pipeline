import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FILE_TABLE_MAP = {
    'olist_orders_dataset.csv': 'raw_orders',
    'olist_order_items_dataset.csv': 'raw_order_items',
    'olist_order_payments_dataset.csv': 'raw_order_payments',
    'olist_order_reviews_dataset.csv': 'raw_order_reviews',
    'olist_customers_dataset.csv': 'raw_customers',
    'olist_products_dataset.csv': 'raw_products',
    'olist_sellers_dataset.csv': 'raw_sellers',
    'olist_geolocation_dataset.csv': 'raw_geolocation',
    'product_category_name_translation.csv': 'raw_category_translation',
}

TIMESTAMP_COLS = {
    'raw_orders': ['order_purchase_timestamp', 'order_approved_at',
                   'order_delivered_carrier_date',
                   'order_delivered_customer_date',
                   'order_estimated_delivery_date'],
    'raw_order_items': ['shipping_limit_date'],
    'raw_order_reviews': ['review_creation_date', 'review_answer_timestamp'],
}

def get_engine():
    url = URL(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema='RAW',
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        role=os.environ['SNOWFLAKE_ROLE'],
    )
    return create_engine(url)

def load_file(engine, filename, table_name, data_dir='/opt/airflow/data/raw'):
    path = f'{data_dir}/{filename}'
    logger.info(f'Reading {path}')
    df = pd.read_csv(path)

    for col in TIMESTAMP_COLS.get(table_name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    logger.info(f'Loading {len(df)} rows into {table_name}')
    df.to_sql(
        name=table_name,
        con=engine,
        schema='raw',
        if_exists='append',
        index=False,
        chunksize=10000,
    )
    logger.info(f'Done: {table_name} ({len(df)} rows)')
    return len(df)

def run_all():
    engine = get_engine()
    total = 0
    for filename, table_name in FILE_TABLE_MAP.items():
        rows = load_file(engine, filename, table_name)
        total += rows
    logger.info(f'ALL DONE. Total rows loaded: {total}')
    return total
