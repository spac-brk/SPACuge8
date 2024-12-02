import pandas as pd
import numpy as np
import pyodbc
from glob import glob
import os

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)

# MySQL connection setup
conn = pyodbc.connect('DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=localhost;DATABASE=weather;UID=brk;PWD=12345678')
cursor = conn.cursor()


# Create a single table with timeResolution
def create_table():
    cursor.execute('DROP TABLE IF EXISTS `elmaps_data`;')

    query = """
        CREATE TABLE `elmaps_data` (
            `time_resolution` VARCHAR(10),
            `datetime_utc` DATETIME,
            `carbon_intensity_direct` FLOAT,
            `carbon_intensity_lca` FLOAT,
            `low_carbon_percentage` FLOAT,
            `renewable_percentage` FLOAT
        );
    """
    cursor.execute(query)


def write_df_to_sql(df: pd.DataFrame, connection: pyodbc.Connection, table_name: str):
    cursor = connection.cursor()

    # Generate column names for SQL insert
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Write rows to the database
    try:
        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row))
        connection.commit()
        print(f"Data successfully written to {table_name}.")
    except Exception as e:
        connection.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()


# Main process
def process_files():
    # Directory containing text files
    directory_path = '.\\data\\Weather\\electricitymaps'
    file_paths = sorted(glob(os.path.join(directory_path, '*.csv')))

    # Create the table
    print("Creating table...")
    create_table()

    # Process each file and insert records
    print("Processing files...")
    elmaps_data = pd.DataFrame()
    for file_path in file_paths:
        print(f"Processing file: {file_path}")
        curr_data = pd.read_csv(file_path)
        curr_data['Time Resolution'] = file_path[39:-4]
        elmaps_data = pd.concat([elmaps_data, curr_data])
    elmaps_data.columns = (elmaps_data.columns
                           .str.replace('gCO₂eq/kWh ', '')
                           .str.replace('(', '')
                           .str.replace(')', '')
                           .str.replace(' ', '_')
                           .str.lower())
    elmaps_data = elmaps_data[['time_resolution',
                               'datetime_utc',
                               'carbon_intensity_direct',
                               'carbon_intensity_lca',
                               'low_carbon_percentage',
                               'renewable_percentage']]
    elmaps_data = elmaps_data.replace({'time_resolution': {'hourly': 'hour',
                                                           'daily': 'day',
                                                           'monthly': 'month',
                                                           'yearly': 'year'}})
    elmaps_data = elmaps_data.replace({np.nan: None})
    write_df_to_sql(elmaps_data, conn, 'elmaps_data')
    print("All data processed and saved.")


# Run the process
try:
    process_files()
finally:
    conn.close()
    print("Database connection closed.")
