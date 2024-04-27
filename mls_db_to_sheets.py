import sqlite3
from dotenv import load_dotenv
import os

def execute_sql_query(full_query):
    # Load environment variables from .env file
    load_dotenv()

    # Get the database path from environment variables
    database_path = os.getenv('DATABASE_PATH')
    if not database_path:
        print("Database path is not set in the .env file.")
        return

    # Connect to the SQLite database
    connection = sqlite3.connect(database_path)
    cursor = connection.cursor()

    try:
        cursor.execute(full_query)
        results = cursor.fetchall()  # Fetches all matching rows
        if results:
            print("Query Results:", len(results))
            for result in results:
                print(result)
        else:
            print("No results found for the given query.")
    except sqlite3.Error as e:
        print("Database error:", e)
    finally:
        # Close the database connection
        connection.close()

# Example usage

# Query to find a property by MLS (ensure secure parameter handling)
mls_query = "SELECT * FROM property_listings WHERE mls = '22-223'"
execute_sql_query(mls_query)

# Query to select all active properties
# status_query = "SELECT * FROM property_listings WHERE status = 'Active'"
# execute_sql_query(status_query)
