import mysql.connector
from mysql.connector import Error
import csv
import os

# Initialize connection and cursor variables
connection = None
cursor = None

# Define the path where you want to save the CSV file
# Ensure you include the file name in the path
output_path = '/Users/vedanggharat/Movies/project/output_data.csv'

try:
    # Establish a connection to the MariaDB database
    connection = mysql.connector.connect(
        host='65.109.17.58',
        user='jnp_prod_user',
        password='Rwmx13Uh3o&n380$56',
        database='jobsnprofiles_2022'  # Replace with your database name
    )

    if connection.is_connected():
        print("Connected to MariaDB")

        # Create a cursor object using the connection
        cursor = connection.cursor()

        # Execute a query
        cursor.execute('SELECT * FROM jnp_jobs')  # Replace with your table name

        # Fetch all results from the executed query
        results = cursor.fetchall()

        # Fetch column names
        column_names = [i[0] for i in cursor.description]

        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Write data to a CSV file
        with open(output_path, 'w', newline='') as file:
            writer = csv.writer(file)
            
            # Write the column headers
            writer.writerow(column_names)
            
            # Write the data
            writer.writerows(results)

        print(f"Data has been written to {output_path}")

except Error as e:
    print(f"Error: {e}")

finally:
    if cursor:
        cursor.close()
    if connection and connection.is_connected():
        connection.close()
    print("Connection closed")


