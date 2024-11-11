import pandas as pd
import json
import sqlite3
import database_interaction
import extract_data

# Create the Database
conn = sqlite3.connect("2023.db")

try:
    database_interaction.create_tables(conn)
except sqlite3.OperationalError:
    database_interaction.delete_all_tables(conn)
    database_interaction.create_tables(conn)

# Load the raw data from the JSON file
with open("raw_data\\2023.json", "r") as raw_2024_file:
    raw_2024 = json.load(raw_2024_file)

extract_data.extract_data(conn, raw_2024)