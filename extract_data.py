import pandas as pd
import json

proficiency_score_adjustment_amount = 20

def fix_proficiency_score_cut_off_points(proficiency_score_cut_off_points_normalized):
    """
    Fix the proficiency score cut off points by adding a startPoint column if it doesn't exist.
    The startPoint is calculated as the scoreCutPoint of the previous level for each unique disciplineId and year.
    
    Parameters:
    proficiency_score_cut_off_points_normalized: pd.DataFrame
    A DataFrame containing the proficiency score cut off points information
    
    Returns:
    pd.DataFrame
    The modified DataFrame with the startPoint column added
    """    
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
    
    # Add the new proficiency level "Just below developing"
    new_rows = []
    for _, row in proficiency_score_cut_off_points_normalized.iterrows():
        if row["level"] == "Developing":
            new_row = row.copy()
            new_row["level"] = "Just below developing"
            new_row["startPoint"] = row["startPoint"] - proficiency_score_adjustment_amount
            new_row["scoreCutPoint"] = row["startPoint"]
            new_rows.append(new_row)
        elif row["level"] == "Exceeding":
            new_row = row.copy()
            new_row["level"] = "Just below exceeding"
            new_row["startPoint"] = row["startPoint"] - proficiency_score_adjustment_amount
            new_row["scoreCutPoint"] = row["startPoint"]
            new_rows.append(new_row)
    
    proficiency_score_cut_off_points_normalized = pd.concat([proficiency_score_cut_off_points_normalized, pd.DataFrame(new_rows)], ignore_index=True)
    
    # Adjust the "Needs additional support" scoreCutPoint to reflect the 20-point change
    proficiency_score_cut_off_points_normalized.loc[proficiency_score_cut_off_points_normalized["level"] == "Needs additional support", "scoreCutPoint"] -= proficiency_score_adjustment_amount

    # Adjust the "Strong" scoreCutPoint to reflect the 20-point change for "Just below exceeding"
    proficiency_score_cut_off_points_normalized.loc[proficiency_score_cut_off_points_normalized["level"] == "Strong", "scoreCutPoint"] -= proficiency_score_adjustment_amount
    
    return proficiency_score_cut_off_points_normalized


def extract_domains(raw_data, df):
    """
    Extract the domains from the raw JSON and insert them into the dataframe.
    
    Parameters:
    raw_data: dict
    The raw JSON data from the NAPLAN file.
    
    df: pd.DataFrame
    The dataframe to insert the domains into.
    
    Returns:
    None
    """
    # Normalize the JSON data
    raw_data = pd.json_normalize(raw_data)

    # Extract the domains column
    domains = raw_data["domains"]
    
    # Further normalize each JSON entry within the domains column
    domains_normalized = pd.json_normalize(domains.explode())
    
    # Insert the normalized domains data into the dataframe
    df = pd.concat([df, domains_normalized], ignore_index=True)
    
    return df


def extract_proficiency(raw_data, df):
    """
    Extract the proficiency data from the raw JSON and insert it into the dataframe.
    
    Parameters:
    raw_data: dict
    The raw JSON data from the NAPLAN file.
    
    df: pd.DataFrame
    The dataframe to insert the proficiency data into.
    
    Returns:
    None
    """
    
    # Normalize the JSON data
    raw_data = pd.json_normalize(raw_data)

    # Extract the proficiencyScoreCutOffPoints column
    proficiency_sortorder = raw_data["proficiencyScoreCutOffPoints"]
    
    # Further normalize each JSON entry within the proficiencySortorder column
    proficiency_sortorder_normalized = pd.json_normalize(proficiency_sortorder.explode())
    
    # Fix the proficiency score cut off points
    proficiency_sortorder_normalized = fix_proficiency_score_cut_off_points(proficiency_sortorder_normalized)
    
    # Insert the normalized proficiencySortorder data into the dataframe
    df = pd.concat([df, proficiency_sortorder_normalized], ignore_index=True)
    
    return df


def extract_questions(raw_data, df, year):
    """
    Extract the questions from the raw JSON and insert them into the dataframe.
    
    Parameters:
    raw_data: dict
    The raw JSON data from the NAPLAN file.
    
    df: pd.DataFrame
    The dataframe to insert the questions into.
    
    Returns:
    None
    """
    
    # Normalize the JSON data
    raw_data = pd.json_normalize(raw_data)

    # Extract the questions column
    questions = raw_data["questions"]
    
    # Further normalize each JSON entry within the questions column
    questions_normalized = pd.json_normalize(questions.explode())

    # Keep only the specified columns
    columns_to_keep = [
        "questionIdentifier",
        "descriptor",
        "domain",
        "subdomain",
        "testLevel",
        "proficiencyLevel",
        "attempts",
        "correct",
        "incorrect",
        "notAttempted"
    ]
    questions_normalized = questions_normalized.filter(items=columns_to_keep)

    # Add the year column
    questions_normalized["year"] = year

    # Insert the normalized questions data into the dataframe
    df = pd.concat([df, questions_normalized], ignore_index=True)
    
    return df


def extract_attempts(raw_data, df):
    """
    Extract the attempts from the raw JSON and insert them into the dataframe.
    
    Parameters:
    raw_data: dict
    The raw JSON data from the NAPLAN file.
    
    df: pd.DataFrame
    The dataframe to insert the attempts into.
    
    Returns:
    None
    """
    
    # Normalise the JSON data
    raw_data = pd.json_normalize(raw_data)

    # Extract the attempts column
    attempts = raw_data["attempts"]
        
    # Further normalise each JSON entry within the attempts column
    attempts_normalised = pd.json_normalize(attempts.explode())

    attempts_normalised = attempts_normalised[attempts_normalised["domain.isWritingTask"] != True]

    # Keep only the specified columns
    columns_to_keep = [
        "answers",
        "attempted", 
        "notAttempted", 
        "correctAttempts", 
        "incorrectAttempts", 
        "testAttemptStatus",
        "scaledScore",
        "student.testLevel",
        "student.metadata.studentLOTE",
        "student.metadata.schoolStudentId",
        "domain.domainName",
        "domain.domainId"
    ]

    # Filter columns that exist in the DataFrame
    existing_columns = [col for col in columns_to_keep if col in attempts_normalised.columns]
    attempts_normalised = attempts_normalised[existing_columns]

    # Filter out where student.metadata.schoolStudentId is Blank, this removes students with no EDID
    attempts_normalised = attempts_normalised[attempts_normalised["student.metadata.schoolStudentId"].notna()]
    
    # Explode the "answers" column to create a separate row for each answer entry and reset the index to ensure proper alignment of rows
    df_exploded = attempts_normalised.explode("answers").reset_index(drop=True)
    
    # Normalise the exploded answers column
    attempts_normalised = pd.json_normalize(df_exploded["answers"].tolist())

    # Drop the original "answers" column from the exploded DataFrame and join it with the normalised answers DataFrame,
    # effectively merging the parsed answer fields as individual columns into the result
    result_df = df_exploded.drop(columns=["answers"]).join(attempts_normalised)

    # Remove unwanted columns
    unwanted_columns = [
        "testAttemptStatus",
        "questionNo",
        "questionID",
        "parallelTestSection",
        "node",
        "locationInTestSection",
        "eventIdentifier",
        "performance",
        "writingResponse",
        "markingSchemeComponents"
    ]
    result_df = result_df.drop(columns=[col for col in unwanted_columns if col in result_df.columns], errors='ignore')

    # Insert the normalized attempts data into the dataframe
    df = pd.concat([df, result_df], ignore_index=True)
    
    return df


def extract_writing_attempts(raw_data, df):
    """
    Extract the writing attempts from the raw JSON and insert them into the dataframe.
    
    Parameters:
    raw_data: dict
    The raw JSON data from the NAPLAN file.
    
    df: pd.DataFrame
    The dataframe to insert the writing attempts into.
    
    Returns:
    None
    """
    
    # Normalize the JSON data
    raw_data = pd.json_normalize(raw_data)

    # Extract the writingAttempts column
    writing_attempts = raw_data["attempts"]
    
    # Further normalize each JSON entry within the writingAttempts column
    writing_attempts_normalised = pd.json_normalize(writing_attempts.explode())

    writing_attempts_normalised = writing_attempts_normalised[writing_attempts_normalised["domain.isWritingTask"] == True]

    # Keep only the specified columns
    columns_to_keep = [
        "answers",
        "attempted", 
        "notAttempted", 
        "correctAttempts", 
        "incorrectAttempts", 
        "testAttemptStatus",
        "scaledScore",
        "student.testLevel",
        "student.metadata.studentLOTE",
        "student.metadata.schoolStudentId",
        "domain.domainName",
        "domain.domainId"
    ]

    # Filter columns that exist in the DataFrame
    existing_columns = [col for col in columns_to_keep if col in attempts_normalised.columns]
    attempts_normalised = attempts_normalised[existing_columns]

    # Filter out where student.metadata.schoolStudentId is Blank, this removes students with no EDID
    attempts_normalised = attempts_normalised[attempts_normalised["student.metadata.schoolStudentId"].notna()]
    
    # Explode the "answers" column to create a separate row for each answer entry and reset the index to ensure proper alignment of rows
    df_exploded = attempts_normalised.explode("answers").reset_index(drop=True)
    
    # Normalise the exploded answers column
    attempts_normalised = pd.json_normalize(df_exploded["answers"].tolist())

    # Drop the original "answers" column from the exploded DataFrame and join it with the normalised answers DataFrame,
    # effectively merging the parsed answer fields as individual columns into the result
    result_df = df_exploded.drop(columns=["answers"]).join(attempts_normalised)

    # Insert the normalized attempts data into the dataframe
    df = pd.concat([df, result_df], ignore_index=True)
    
    return df