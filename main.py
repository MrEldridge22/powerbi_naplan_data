import pandas as pd
import json
import extract_data
import os

# Define the DataFrames
domainsDF = pd.DataFrame()
proficiencySortorder = pd.DataFrame()
questions = pd.DataFrame()
attempts = pd.DataFrame()

# Load the raw data from the JSON file
dataFilePath = "raw_data\\"
dataFiles = os.listdir(dataFilePath)
for file in dataFiles:
    if file.endswith(".json"):
        print(f"Processing file: {file}")
        with open(os.path.join(dataFilePath, file), "r") as raw_json_file:
            raw = json.load(raw_json_file)
            domainsDF = extract_data.extract_domains(raw, domainsDF)
            proficiencySortorder = extract_data.extract_proficiency(raw, proficiencySortorder)
            questions = extract_data.extract_questions(raw, questions)
            attempts = extract_data.extract_attempts(raw, attempts)


