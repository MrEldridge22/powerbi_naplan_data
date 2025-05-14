import pandas as pd
import json
import extract_data
import os

# Define the DataFrames
domainsDF = pd.DataFrame()
proficiencySortorder = pd.DataFrame()
questions = pd.DataFrame()
attempts = pd.DataFrame()
writing_attempts = pd.DataFrame()

# Load the raw data from the JSON file
dataFilePath = "raw_data\\"
dataFiles = os.listdir(dataFilePath)
for file in dataFiles:
    if file.endswith(".json"):
        print(f"Processing file: {file}")
        year = file.split(" ")[0]
        with open(os.path.join(dataFilePath, file), "r") as raw_json_file:
            raw = json.load(raw_json_file)
            domainsDF = extract_data.extract_domains(raw, domainsDF)
            proficiencySortorder = extract_data.extract_proficiency(raw, proficiencySortorder)
            questions = extract_data.extract_questions(raw, questions, year)
            attempts = extract_data.extract_attempts(raw, attempts)
            writing_attempts = extract_data.extract_writing_attempts(raw, writing_attempts)
            print(f"Finished processing file: {file}")

# Check if the questionIdentifier has duplicates with different descriptor values
duplicates_check = questions.groupby('questionIdentifier')['descriptor'].nunique().reset_index()
questionIds_with_different_descriptors = duplicates_check[duplicates_check['descriptor'] > 1]['questionIdentifier'].tolist()

if questionIds_with_different_descriptors:
    print(f"Found {len(questionIds_with_different_descriptors)} questionIdentifiers with different descriptor values:")
    
    # Create a DataFrame to store all duplicates
    all_duplicates = pd.DataFrame()
    
    # Collect all the duplicates in one DataFrame
    for qid in questionIds_with_different_descriptors:
        different_descriptors = questions[questions['questionIdentifier'] == qid][['questionIdentifier', 'descriptor', 'year']].drop_duplicates()
        all_duplicates = pd.concat([all_duplicates, different_descriptors], ignore_index=True)
    
    # Export the duplicates to a CSV file
    all_duplicates.to_csv("duplicate_descriptors.csv", index=False)
    print(f"Exported {len(all_duplicates)} duplicate records to 'duplicate_descriptors.csv'")
else:
    print("No questionIdentifiers found with different descriptor values.")

# Export the full questions dataset
questions.to_csv("questions.csv", index=False)
# Export Student Responses to csv's, have split writing responses into it's own file
attempts.to_csv("attempts.csv", index=False)
writing_attempts.to_csv("writing_attempts.csv", index=False)
# Export the proficiency sort order
proficiencySortorder.to_csv("proficiencySortorder.csv", index=False)
# Export the domains
domainsDF.to_csv("domains.csv", index=False)