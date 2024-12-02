import pyodbc
import json
from glob import glob
import os

# MySQL connection setup
conn = pyodbc.connect('DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=localhost;DATABASE=weather;UID=brk;PWD=12345678')
cursor = conn.cursor()


# Create a single table with timeResolution
def create_table():
    query = """
        CREATE TABLE IF NOT EXISTS `weather_data` (
            `from` DATETIME,
            `to` DATETIME,
            `timeResolution` VARCHAR(10),
            `parameterId` VARCHAR(255),
            `value` FLOAT
        );
    """
    cursor.execute(query)


# Insert records into the table
def insert_record(record):
    props = record["properties"]
    query = """
        INSERT INTO `weather_data` (`from`, `to`, `timeResolution`, `parameterId`, `value`)
        VALUES (?, ?, ?, ?, ?)
    """
    cursor.execute(query, (props["from"], props["to"],
                           props["timeResolution"],
                           props["parameterId"],
                           props["value"]))


# Main process
def process_files():
    # Directory containing text files
    directory_path = './data/Weather/DMI'
    file_paths = sorted(glob(os.path.join(directory_path, '*.txt')))

    # Create the table
    print("Creating table...")
    create_table()

    # Process each file and insert records
    print("Processing files...")
    for file_path in file_paths:
        print(f"Processing file: {file_path}")
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    record = json.loads(line.strip())
                    insert_record(record)
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error in file {file_path}: {e}")

    conn.commit()
    print("All data processed and saved.")


# Run the process
try:
    process_files()
finally:
    conn.close()
    print("Database connection closed.")