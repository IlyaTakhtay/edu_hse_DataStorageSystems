from utils import get_connection


def test():
    try:
        conn = get_connection()
        print("Connected!")
        with conn.cursor() as cur:
            cur.execute("SHOW search_path;")
            search_path = cur.fetchone()[0]
            print(f"Search path: {search_path***REMOVED***")

            cur.execute("SELECT current_database(), current_schema();")
            db, schema = cur.fetchone()
            print(f"Database: {db***REMOVED***")
            print(f"Current schema: {schema***REMOVED***")

        conn.close()
        print("Connection test successful!")

    except Exception as e:
        print(f"Connection failed: {e***REMOVED***")


if __name__ == "__main__":
    test()
