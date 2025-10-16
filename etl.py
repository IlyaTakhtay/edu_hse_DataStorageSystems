"""
ETL процессы для загрузки данных в Data Vault
"""

import logging
import pandas as pd
from datetime import datetime
from io import StringIO
from pathlib import Path

from utils import execute_sql, execute_sql_file
from config import RECORD_SOURCE

logger = logging.getLogger(__name__)


def create_tables(conn):
    """Создать все таблицы Data Vault"""
    logger.info("Creating Data Vault tables")
    
    execute_sql_file(conn, 'sql/01_ddl_staging.sql')
    execute_sql_file(conn, 'sql/02_ddl_hubs.sql')
    execute_sql_file(conn, 'sql/03_ddl_satellites.sql')
    execute_sql_file(conn, 'sql/04_ddl_links.sql')
    
    logger.info("Tables created successfully")


def load_staging(conn, csv_path):
    """Загрузить данные в staging таблицу"""
    logger.info("Loading staging data")
    
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded data from {csv_path}")
    
    # Переименовываем колонки: "Ship Mode" -> "ship_mode"
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
    
    # Добавляем служебные поля
    df['load_dts'] = datetime.now()
    df['record_source'] = RECORD_SOURCE
    
    # Очистка staging
    execute_sql(conn, "TRUNCATE TABLE stg_orders;")
    
    # Загрузка через COPY
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='|')
    buffer.seek(0)
    
    with conn.cursor() as cur:
        cur.copy_from(buffer, 'stg_orders', sep='|', columns=list(df.columns), null='')
        conn.commit()
    
    logger.info(f"Loaded {len(df)} rows into staging")
    return len(df)


def load_hubs(conn):
    """Загрузить данные в Hub таблицы"""
    logger.info("Loading hubs")
    execute_sql_file(conn, 'sql/05_load_hubs.sql')
    logger.info("Hubs loaded successfully")


def load_links(conn):
    """Загрузить данные в Link таблицы"""
    logger.info("Loading links")
    execute_sql_file(conn, 'sql/06_load_links.sql')
    logger.info("Links loaded successfully")


def load_satellites(conn):
    """Загрузить данные в Satellite таблицы"""
    logger.info("Loading satellites")
    execute_sql_file(conn, 'sql/07_load_satellites.sql')
    logger.info("Satellites loaded successfully")
