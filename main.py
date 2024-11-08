import pandas as pd
import json
import sqlite3

# Create the Database
conn = sqlite3.connect("2024.db")

# Load the raw data from the JSON file
with open("raw_data\\2024.json", "r") as raw_2024_file:
    raw_2024 = json.load(raw_2024_file)

# Normalize the JSON data
raw_2024 = pd.json_normalize(raw_2024)

# Extract the domains column
domains = raw_2024["domains"]
# Further normalize each JSON entry within the domains column
domains_normalized = pd.json_normalize(domains.explode())

# Extract the subdomains column
sub_domains = raw_2024["subdomains"]
# Further normalize each JSON entry within the subdomains column
sub_domains_normalized = pd.json_normalize(sub_domains.explode())

proficiency_score_cut_off_points = raw_2024["proficiencyScoreCutOffPoints"]
proficiency_score_cut_off_points_normalized = pd.json_normalize(proficiency_score_cut_off_points.explode())

questions = raw_2024["questions"]
questions_normalized = pd.json_normalize(questions.explode())

attempts = raw_2024["attempts"]
attempts_normalized = pd.json_normalize(attempts.explode())

answers_normalized = pd.json_normalize(attempts_normalized['answers'].explode())
# Merge the normalized answers back into the attempts_normalized DataFrame
answers_normalized.columns = ['answers_' + col for col in answers_normalized.columns]
attempts_normalized = attempts_normalized.drop(columns=['answers']).join(answers_normalized)

attempts_normalized.to_csv("attempts_normalized.csv")

print(attempts_normalized)