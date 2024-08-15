import psycopg2
import pandas as pd

# Database connection parameters
db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'PASS@KEY1',
    'host': 'localhost',  # Change if your database is on a different host
    'port': '5432'        # Change if your PostgreSQL server uses a different port
}

# Path to the CSV file
csv_file_path = "datasets/Global_EV_Data_2024.csv"

# SQL command to create a table
create_table_command = """
CREATE TABLE IF NOT EXISTS ev_table(
    id SERIAL PRIMARY KEY,
    region VARCHAR(50),
    category VARCHAR(50),
    parameter VARCHAR(50),
    mode VARCHAR(50),
    powertrain VARCHAR(50),
    year INT,
    unit VARCHAR(50),
    value NUMERIC
);
"""

# SQL command to insert data
insert_data_command = """
INSERT INTO ev_table (region, category, parameter, mode, powertrain, year, unit, value)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

def create_table():
    """Create the table in the PostgreSQL database."""
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(create_table_command)
        print("Table created successfully.")
        connection.commit()
    except Exception as error:
        print(f"Error creating table: {error}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def load_csv_and_insert_data():
    """Load data from CSV and insert into the PostgreSQL table."""
    connection = None
    cursor = None
    try:
        # Load CSV data into a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Ensure that the DataFrame has the correct columns
        required_columns = {'region', 'category', 'parameter', 'mode', 'powertrain', 'year', 'unit', 'value'}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"CSV file must contain the following columns: {required_columns}")

        # Truncate 'unit' values that exceed the maximum length (20 characters)
        max_length = 50  # Length defined in the table schema
        # Clean 'unit' values by stripping extra spaces and truncating
        df['unit'] = df['unit'].astype(str).str.strip().apply(lambda x: x[:max_length])


        # Print the lengths of the 'unit' column to verify truncation
        df['unit_length'] = df['unit'].apply(len)
        print(f"Maximum length of 'unit' values: {df['unit_length'].max()}")
        if df['unit_length'].max() > max_length:
            raise ValueError("Truncation failed, some 'unit' values still exceed the maximum length.")

        # Convert DataFrame to list of tuples for insertion
        data_to_insert = df.drop(columns=['unit_length']).to_records(index=False).tolist()

        # Establish a connection to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Insert data into the table
        cursor.executemany(insert_data_command, data_to_insert)
        print(f"{len(data_to_insert)} rows inserted successfully.")

        # Commit the transaction
        connection.commit()

    except Exception as error:
        print(f"Error inserting data: {error}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    create_table()  # Create the table if it doesn't exist
    load_csv_and_insert_data()  # Load data from CSV and insert it into the table
