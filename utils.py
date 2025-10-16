"""
Вспомогательные функции
"""

import logging
import psycopg2
from pathlib import Path
from config import DB_CONFIG, SCHEMA_NAME

logger = logging.getLogger(__name__)


def get_connection():
    """Подключение к Greenplum с установкой схемы"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {SCHEMA_NAME};")
        conn.commit()
    
    logger.debug(f"Connection established, schema set to {SCHEMA_NAME}")
    return conn


def execute_sql(conn, sql):
    """Выполнить SQL запрос"""
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def execute_sql_file(conn, filepath):
    """Выполнить SQL из файла"""
    path = Path(filepath)
    logger.debug(f"Executing SQL file: {path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    with conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def print_table_stats(conn):
    """Вывести статистику по таблицам Data Vault"""
    query = f"""
        SELECT 'Hubs' as layer, 'hub_location' as name, COUNT(*) FROM {SCHEMA_NAME}.hub_location
        UNION ALL SELECT 'Hubs', 'hub_product', COUNT(*) FROM {SCHEMA_NAME}.hub_product
        UNION ALL SELECT 'Hubs', 'hub_category', COUNT(*) FROM {SCHEMA_NAME}.hub_category
        UNION ALL SELECT 'Hubs', 'hub_ship_mode', COUNT(*) FROM {SCHEMA_NAME}.hub_ship_mode
        UNION ALL SELECT 'Hubs', 'hub_segment', COUNT(*) FROM {SCHEMA_NAME}.hub_segment
        UNION ALL SELECT 'Links', 'link_product_hierarchy', COUNT(*) FROM {SCHEMA_NAME}.link_product_hierarchy
        UNION ALL SELECT 'Links', 'link_order_line', COUNT(*) FROM {SCHEMA_NAME}.link_order_line
        UNION ALL SELECT 'Satellites', 'sat_location_details', COUNT(*) FROM {SCHEMA_NAME}.sat_location_details
        UNION ALL SELECT 'Satellites', 'sat_product_category', COUNT(*) FROM {SCHEMA_NAME}.sat_product_category
        UNION ALL SELECT 'Satellites', 'sat_order_line_metrics', COUNT(*) FROM {SCHEMA_NAME}.sat_order_line_metrics
        ORDER BY layer, name;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
    
    print("\nData Vault Statistics:")
    for layer, name, count in results:
        print(f"{layer:<12} {name:<30} {count:>6} rows")
    print()
