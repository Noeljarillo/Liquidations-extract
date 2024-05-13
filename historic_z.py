import requests
import os
import sqlite3
import pandas as pd
from zipfile import ZipFile
from datetime import datetime, timedelta

# Establish a connection to the SQLite database
conn = sqlite3.connect('liquidations.db')
c = conn.cursor()
c.execute('''
CREATE TABLE IF NOT EXISTS liquidations (
    time DATETIME,
    side TEXT,
    order_type TEXT,
    time_in_force TEXT,
    original_quantity REAL,
    price REAL,
    average_price REAL,
    order_status TEXT,
    last_fill_quantity REAL,
    accumulated_fill_quantity REAL)
''')
print("Database connection established and table created.")

# Define the date range
start_date = datetime(2023, 6, 25)
end_date = datetime(2024, 5, 11)

# Function to generate date strings in the required format
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

for single_date in daterange(start_date, end_date):
    date_str = single_date.strftime("%Y-%m-%d")
    file_name = f"BTCUSDT-liquidationSnapshot-{date_str}.zip"
    url = f"https://data.binance.vision/data/futures/um/daily/liquidationSnapshot/BTCUSDT/{file_name}"

    response = requests.head(url)
    if response.status_code == 200:
        print(f"File found for {date_str}, downloading...")
        # Only proceed if the file exists
        response = requests.get(url)
        with open(file_name, 'wb') as f:
            f.write(response.content)

        with ZipFile(file_name, 'r') as zip_ref:
            zip_ref.extractall(".")
            csv_file_name = file_name.replace('.zip', '.csv')
            print(f"Extracting {csv_file_name}...")
            df = pd.read_csv(csv_file_name)
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            df.to_sql('liquidations', conn, if_exists='append', index=False)
            print(f"Data from {csv_file_name} imported into the database.")
        
        os.remove(file_name)
        os.remove(csv_file_name)
        print(f"Cleaned up downloaded and extracted files for {date_str}.")
    else:
        print(f"No data available for {date_str}")

# Close the database connection
conn.close()
print("Database connection closed.")
