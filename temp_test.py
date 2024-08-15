import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'PASS@KEY1',
    'host': 'localhost',  # Change if your database is on a different host
    'port': '5432'        # Change if your PostgreSQL server uses a different port
}

# SQL command to create a table
create_table_command = """
CREATE TABLE IF NOT EXISTS ev_table (
    id SERIAL PRIMARY KEY,
    region VARCHAR(50),
    category VARCHAR(50),
    parameter VARCHAR(50),
    mode VARCHAR(50),
    powertrain VARCHAR(50),
    year INT,
    unit VARCHAR(20),
    value NUMERIC
);
"""

def create_table():
    # Establish a connection to the database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Execute the SQL command to create the table
        cursor.execute(create_table_command)
        print("Table created successfully.")

        # Commit the transaction
        connection.commit()

    except Exception as error:
        print(f"Error creating table: {error}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    create_table()

