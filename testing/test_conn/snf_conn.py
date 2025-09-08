# test_snowflake_connection.py

from DBconnection import db_connections

def test_connection():
    try:
        # Create connection (change sourcedb to "SNOWFLAKE" or "SNOWFLAKE_WEST")
        conn = db_connections("SNOWFLAKE", "Service")

        if conn is None:
            print("❌ Connection failed: returned None")
            return

        # Create cursor
        cursor = conn.cursor()

        # Run a test query
        cursor.execute("""
            SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(),
                   CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
        """)
        result = cursor.fetchone()

        cursor.execute("""
            SHOW DATABASES;
        """)
        result1 = cursor.fetchone()

        print("\n✅ Snowflake Connection Successful!")
        print(f"Snowflake Version  : {result[0]}")
        print(f"User               : {result[1]}")
        print(f"Role               : {result[2]}")
        print(f"Warehouse          : {result[3]}")
        print(f"Database           : {result[4]}")
        print(f"Schema             : {result[5]}")
        print(f"Database           : {result1}")



    except Exception as e:
        print(f"❌ Error during connection test: {e}")

    finally:
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    test_connection()
