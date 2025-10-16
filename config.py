DB_CONFIG = {
    "host": ***REMOVED***,
    "port": 6432,
    "database": "hse",
    "user": "student47",
    "password": ***REMOVED***,
    "options": "-c search_path=student47",  # автоматически использовать схему student47
}


RECORD_SOURCE = "SampleSuperstore_csv"  # Название источника загрузки

SCHEMA_NAME = "student47"

SOURCE_PATH = "./SampleSuperstore.csv"

