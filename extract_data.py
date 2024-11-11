import pandas as pd
import database_interaction


def fix_proficiency_score_cut_off_points(proficiency_score_cut_off_points_normalized):
    if "startPoint" not in proficiency_score_cut_off_points_normalized.columns:
        # Define the order of proficiency levels
        level_order = ["Needs additional support", "Developing", "Strong", "Exceeding"]
        
        # Sort the DataFrame by disciplineId, year, and proficiency level
        proficiency_score_cut_off_points_normalized["level_order"] = proficiency_score_cut_off_points_normalized["level"].apply(lambda x: level_order.index(x))
        proficiency_score_cut_off_points_normalized = proficiency_score_cut_off_points_normalized.sort_values(by=["disciplineId", "year", "level_order"])
        
        # Calculate the startPoint
        proficiency_score_cut_off_points_normalized["startPoint"] = proficiency_score_cut_off_points_normalized.groupby(["disciplineId", "year"])["scoreCutPoint"].shift(1)
        
        # Fill NaN values in startPoint with 0 or any other appropriate value
        proficiency_score_cut_off_points_normalized["startPoint"] = proficiency_score_cut_off_points_normalized["startPoint"].fillna(0)
        
        # Drop the temporary level_order column
        proficiency_score_cut_off_points_normalized = proficiency_score_cut_off_points_normalized.drop(columns=["level_order"])

    # Change the column name from disciplineId to domainId
    if "disciplineId" in proficiency_score_cut_off_points_normalized.columns:
        proficiency_score_cut_off_points_normalized = proficiency_score_cut_off_points_normalized.rename(columns={"disciplineId": "domainId"})
    
    return proficiency_score_cut_off_points_normalized


def extract_data(conn, raw_data):

    # Normalize the JSON data
    raw_data = pd.json_normalize(raw_data)

    # Extract the domains column
    domains = raw_data["domains"]
    # Further normalize each JSON entry within the domains column
    domains_normalized = pd.json_normalize(domains.explode())
    database_interaction.insert_domains(conn, domains_normalized)

    # Extract the subdomains column
    sub_domains = raw_data["subdomains"]
    # Further normalize each JSON entry within the subdomains column
    sub_domains_normalized = pd.json_normalize(sub_domains.explode())
    database_interaction.insert_subdomains(conn, sub_domains_normalized)

    # Exract the proficiencyScoreCutOffPoints column, need to add in the cut off points for just belows
    proficiency_score_cut_off_points = raw_data["proficiencyScoreCutOffPoints"]
    proficiency_score_cut_off_points_normalized = pd.json_normalize(proficiency_score_cut_off_points.explode())
    database_interaction.insert_proficiency_score_cut_off_points(conn, fix_proficiency_score_cut_off_points(proficiency_score_cut_off_points_normalized))

    # Extract the questions column
    questions = raw_data["questions"]
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

    # Ensure that scoreDescriptions is a list of dictionaries
    marking_scheme_normalized_exploded["scoreDescriptions"] = marking_scheme_normalized_exploded["scoreDescriptions"].apply(lambda x: eval(x) if isinstance(x, str) else x)

    # Create a list to hold the expanded rows
    expanded_rows = []

    # Iterate over each row and split the scoreDescriptions
    for index, row in marking_scheme_normalized_exploded.iterrows():
        if isinstance(row["scoreDescriptions"], list):
            for item in row["scoreDescriptions"]:
                for score, description in item.items():
                    new_row = row.copy()
                    new_row["scoreD"] = score
                    new_row["sDescription"] = description
                    expanded_rows.append(new_row)

    # Create a DataFrame from the expanded rows
    score_descriptions_normalized = pd.DataFrame(expanded_rows)

    # Drop the original scoreDescriptions column
    score_descriptions_normalized = score_descriptions_normalized.drop(columns=["scoreDescriptions"])

    database_interaction.insert_writing_marking_scheme(conn, score_descriptions_normalized)

    # Attempts
    attempts = raw_data["attempts"]
    attempts_normalized = pd.json_normalize(attempts.explode())

    # Student information
    students_df = attempts_normalized[["student.studentId", "student.metadata.studentLOTE", "student.metadata.schoolStudentId"]]
    # Remove duplicates
    students_df = students_df.drop_duplicates()
    database_interaction.insert_students(conn, students_df)
    database_interaction.insert_students_scores(conn, attempts_normalized)

    # Extract the answers column and the corresponding student.studentId column
    answers_df = attempts_normalized[["student.studentId", "student.testLevel", "answers"]]

    # Explode the answers column
    answers_exploded = answers_df.explode("answers")

    # Normalize the exploded answers column
    answers_normalized = pd.json_normalize(answers_exploded["answers"])

    # Add the student.studentId column back to the normalized answers DataFrame
    answers_normalized["student.studentId"] = answers_exploded["student.studentId"].values
    answers_normalized["student.testLevel"] = answers_exploded["student.testLevel"].values

    # Non Writing Responses
    database_interaction.insert_attempts(conn, answers_normalized[answers_normalized["writingResponse"].isna()])

    # Writing Responses
    writing_responses = answers_normalized[answers_normalized["writingResponse"].notna()]
    # Explode the writingResponses column
    writing_responses_exploded = writing_responses.explode("markingSchemeComponents")
    # Normalize the exploded writingResponses column
    writing_responses_normalized = pd.json_normalize(writing_responses_exploded["markingSchemeComponents"])
    # Add the other columns back to the normalized writing responses DataFrame
    for col in writing_responses_exploded.columns:
        if col != "markingSchemeComponents":
            writing_responses_normalized[col] = writing_responses_exploded[col].values

    database_interaction.insert_writing_responses(conn, writing_responses_normalized)


    answers_normalized = pd.json_normalize(attempts_normalized['answers'].explode())
    # Merge the normalized answers back into the attempts_normalized DataFrame
    answers_normalized.columns = ['answers_' + col for col in answers_normalized.columns]
    attempts_normalized = attempts_normalized.drop(columns=['answers']).join(answers_normalized)
