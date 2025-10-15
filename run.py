"""
Data Vault 2.0 ETL Runner
Usage: python run.py [csv_path]
"""

import sys
import logging

from utils import get_connection, print_table_stats
from etl import create_tables, load_staging, load_hubs, load_links, load_satellites
from config import SOURCE_PATH

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    csv_path = SOURCE_PATH

    logger.info(f"Starting ETL pipeline with source: {csv_path***REMOVED***")

    try:
        conn = get_connection()
        logger.info("Database connection established")

        create_tables(conn)
        rows = load_staging(conn, csv_path)
        load_hubs(conn)
        load_links(conn)
        load_satellites(conn)

        print_table_stats(conn)

        conn.close()
        logger.info(f"ETL pipeline completed. Processed {rows***REMOVED*** rows")
        return 0

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e***REMOVED***", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
