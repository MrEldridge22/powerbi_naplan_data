import pandas as pd
import json
import sqlite3
import database_interaction

# Create the Database
conn = sqlite3.connect("2024.db")

try:
    database_interaction.create_tables(conn)
except sqlite3.OperationalError:
    database_interaction.delete_all_tables(conn)
    database_interaction.create_tables(conn)

# Load the raw data from the JSON file
with open("raw_data\\2024.json", "r") as raw_2024_file:
    raw_2024 = json.load(raw_2024_file)

# Normalize the JSON data
raw_2024 = pd.json_normalize(raw_2024)

# Extract the domains column
domains = raw_2024["domains"]
# Further normalize each JSON entry within the domains column
domains_normalized = pd.json_normalize(domains.explode())
database_interaction.insert_domains(conn, domains_normalized)

# Extract the subdomains column
sub_domains = raw_2024["subdomains"]
# Further normalize each JSON entry within the subdomains column
sub_domains_normalized = pd.json_normalize(sub_domains.explode())
database_interaction.insert_subdomains(conn, sub_domains_normalized)

# Exract the proficiencyScoreCutOffPoints column, need to add in the cut off points for just belows
proficiency_score_cut_off_points = raw_2024["proficiencyScoreCutOffPoints"]
proficiency_score_cut_off_points_normalized = pd.json_normalize(proficiency_score_cut_off_points.explode())
database_interaction.insert_proficiency_score_cut_off_points(conn, proficiency_score_cut_off_points_normalized)

# Extract the questions column
questions = raw_2024["questions"]
questions_normalized = pd.json_normalize(questions.explode())
# questions_normalized.to_csv("questions_normalized.csv")
database_interaction.insert_questions(conn, questions_normalized)

### ISSUE STARTS HERE ###
# Extract writing marking scheme
writing_questions = questions_normalized[questions_normalized["domain"] == "Writing"]

# Explode the markingSchemeComponents column
writing_questions_exploded = writing_questions.explode("markingSchemeComponents")

# Normalize the exploded markingSchemeComponents column
marking_scheme_normalized = pd.json_normalize(writing_questions_exploded["markingSchemeComponents"])

# Add the other columns back to the normalized marking scheme DataFrame
for col in writing_questions_exploded.columns:
    if col != "markingSchemeComponents":
        marking_scheme_normalized[col] = writing_questions_exploded[col].values

# Explode the scoreDescriptions column
marking_scheme_normalized_exploded = marking_scheme_normalized.explode("scoreDescriptions")

# Normalize the exploded scoreDescriptions column
score_descriptions_normalized = pd.json_normalize(marking_scheme_normalized_exploded["scoreDescriptions"])

# Split the scoreDescriptions into score and description
score_descriptions_normalized = score_descriptions_normalized.apply(lambda x: pd.Series(x), axis=1)
score_descriptions_normalized.columns = ['score', 'description']

# Add the other columns back to the normalized score descriptions DataFrame
for col in marking_scheme_normalized_exploded.columns:
    if col != "scoreDescriptions":
        score_descriptions_normalized[col] = marking_scheme_normalized_exploded[col].values

# Debug print to check the structure of score_descriptions_normalized
print(score_descriptions_normalized.head())

# Save the score_descriptions_normalized DataFrame to a CSV file
score_descriptions_normalized.to_csv("score_descriptions_normalized.csv", index=False)
### ISSUE ENDS HERE ###

attempts = raw_2024["attempts"]
attempts_normalized = pd.json_normalize(attempts.explode())

# Student information
students_df = attempts_normalized[["student.studentId", "student.metadata.studentLOTE", "student.metadata.schoolStudentId"]]
# Remove duplicates
students_df = students_df.drop_duplicates()
database_interaction.insert_students(conn, students_df)
database_interaction.insert_students_scores(conn, attempts_normalized)

# Extract the answers column and the corresponding student.studentId column
answers_df = attempts_normalized[["student.studentId", "answers"]]

# Explode the answers column
answers_exploded = answers_df.explode("answers")

# Normalize the exploded answers column
answers_normalized = pd.json_normalize(answers_exploded["answers"])

# Add the student.studentId column back to the normalized answers DataFrame
answers_normalized["student.studentId"] = answers_exploded["student.studentId"].values

# Non Writing Responses
database_interaction.insert_attempts(conn, answers_normalized[answers_normalized["writingResponse"].isna()])
# Writing Responses
writing_responses = answers_normalized[answers_normalized["writingResponse"].notna()]


answers_normalized = pd.json_normalize(attempts_normalized['answers'].explode())
# Merge the normalized answers back into the attempts_normalized DataFrame
answers_normalized.columns = ['answers_' + col for col in answers_normalized.columns]
attempts_normalized = attempts_normalized.drop(columns=['answers']).join(answers_normalized)
