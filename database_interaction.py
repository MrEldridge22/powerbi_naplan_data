import sqlite3


TABLE_PARAMETER = "{TABLE_PARAMETER}"
DROP_TABLE_SQL = f"DROP TABLE {TABLE_PARAMETER};"
GET_TABLES_SQL = "SELECT name FROM sqlite_schema WHERE type='table';"


def delete_all_tables(con):
    tables = get_tables(con)
    delete_tables(con, tables)


def get_tables(con):
    cur = con.cursor()
    cur.execute(GET_TABLES_SQL)
    tables = cur.fetchall()
    cur.close()
    
    return tables


def delete_tables(con, tables):
    cur = con.cursor()
    for table, in tables:
        if table == "sqlite_sequence":
            continue
        else:
            sql = DROP_TABLE_SQL.replace(TABLE_PARAMETER, table)
            cur.execute(sql)
    cur.close()


def create_tables(conn):

    # Create the domains table
    conn.execute("""
        CREATE TABLE domains (
            domainId TEXT PRIMARY KEY,
            domainName TEXT,
            isWritingTask BOOLEAN
        )
    """)
    
    # Create the subdomains table
    conn.execute("""
        CREATE TABLE subdomains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            title TEXT,
            domainId TEXT,
            FOREIGN KEY (domainId) REFERENCES domains(domainId)
        )
    """)
    
    # Create the proficiency_score_cut_off_points table
    conn.execute("""
        CREATE TABLE proficiency_score_cut_off_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            startPoint REAL,
            scoreCutPoint REAL,
            year INTEGER,
            domainId TEXT,
            FOREIGN KEY (domainId) REFERENCES domains(domainId)
    )
    """)
    
    # Create the questions table
    conn.execute("""
        CREATE TABLE questions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            questionId TEXT,
            eventIdentifier TEXT,
            questionIdentifier TEXT,
            nodeIdentifier TEXT,
            descriptor TEXT,
            domain TEXT,
            domainId TEXT,
            subdomain TEXT,
            subdomainAbbr TEXT,
            subdomain3 TEXT,
            curriculumContentCode TEXT,
            curriculumContentUrl TEXT,
            exemplarItem TEXT,
            testLevel INTEGER,
            difficulty INTEGER,
            proficiencyLevel TEXT,
            attempts INTEGER,
            correct INTEGER,
            incorrect INTEGER,
            notAttempted INTEGER,
            correctPercentage REAL,
            domainAndYearLevelAttempts INTEGER,
            attemptedPercentage REAL,
            parallelTestSection TEXT,
            locationInTestSection INTEGER,
            FOREIGN KEY (domainId) REFERENCES domains(domainId)
        )
    """)

    # Create the students table
    conn.execute("""
        CREATE TABLE students (
            studentId TEXT PRIMARY KEY,
            studentLOTE TEXT,
            schoolStudentId TEXT
        )
    """)

    # Create writing_marking_scheme table
    conn.execute("""
        CREATE TABLE writing_marking_scheme (
            markingSchemeId TEXT,
            questionId TEXT,
            name TEXT,
            description TEXT,
            domainId TEXT,
            testLevel INTEGER,
            proficiency TEXT,
            scoreD INTEGER,
            sDescription TEXT,
            FOREIGN KEY (questionId) REFERENCES writing_questions(questionId),
            FOREIGN KEY (domainId) REFERENCES domains(domainId),
            PRIMARY KEY (markingSchemeId, testLevel, scoreD)
        )
    """)
    
    # Create table student_scores
    conn.execute("""
        CREATE TABLE student_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            studentId TEXT,
            domainId TEXT,
            possibleRawScore REAL,
            studentRawScore REAL,
            scaledScore REAL,
            FOREIGN KEY (domainId) REFERENCES domains(domainId),
            FOREIGN KEY (studentId) REFERENCES students(studentId)
        )
    """)

    # Create the attempts table, this is the student reponses to questions
    conn.execute("""
        CREATE TABLE attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            studentId TEXT,
            correct BOOLEAN,
            answeredOn TEXT,
            questionId TEXT,
            questionNo INTEGER,
            parallelTestSection TEXT,
            node TEXT,
            FOREIGN KEY (questionId) REFERENCES questions(questionId),
            FOREIGN KEY (studentId) REFERENCES students(studentId)
        )
    """)
    
    # Create the writing_responses table
    conn.execute("""
        CREATE TABLE writing_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            studentId TEXT,
            writingRespose TEXT,
            question_id INTEGER,
            markingSchemeId TEXT,
            score INTEGER,
            testLevel INTERGER,
            FOREIGN KEY (question_id) REFERENCES questions(id),
            FOREIGN KEY (studentId) REFERENCES students(studentId),
            FOREIGN KEY (score, testLevel, markingSchemeId) REFERENCES writing_marking_scheme(markingSchemeId, testLevel, scoreD)
        )
    """)

    conn.commit()


def insert_domains(conn, domains_df):
    """
    Insert the domains into the domains table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    domains_df: pd.DataFrame
    A DataFrame containing the domain information
        
    Returns:
    None
    """
    for _, row in domains_df.iterrows():
        conn.execute("""
            INSERT INTO domains (domainId, domainName, isWritingTask) VALUES (?, ?, ?)
        """, (row["domainId"], row["domainName"], row["isWritingTask"]))
    conn.commit()


def insert_subdomains(conn, subdomains_df):
    """
    Insert the subdomains into the subdomains table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    subdomains_df: pd.DataFrame
    A DataFrame containing the subdomain information
        
    Returns:
    None
    """
    for _, row in subdomains_df.iterrows():
        conn.execute("""
            INSERT INTO subdomains (domain, title, domainId) VALUES (?, ?, ?)
        """, (row["domain"], row["title"], row["domainId"]))
    conn.commit()


def insert_proficiency_score_cut_off_points(conn, proficiency_score_cut_off_points_df):
    """
    Insert the proficiency score cut off points into the proficiency_score_cut_off_points table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    proficiency_score_cut_off_points_df: pd.DataFrame
    A DataFrame containing the proficiency score cut off points information
        
    Returns:
    None
    """
    for _, row in proficiency_score_cut_off_points_df.iterrows():
        conn.execute("""
            INSERT INTO proficiency_score_cut_off_points (level, startPoint, scoreCutPoint, year, domainId) VALUES (?, ?, ?, ?, ?)
        """, (row["level"], row["startPoint"], row["scoreCutPoint"], row["year"], row["domainId"]))
    conn.commit()


def insert_questions(conn, questions_df):
    """
    Insert the questions into the questions table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    questions_df: pd.DataFrame
    A DataFrame containing the question information
        
    Returns:
    None
    """
    for _, row in questions_df.iterrows():
        conn.execute("""
            INSERT INTO questions (
                    questionId,
                    eventIdentifier,
                    questionIdentifier,
                    nodeIdentifier,
                    descriptor,
                    domain,
                    domainId,
                    subdomain,
                    subdomainAbbr,
                    subdomain3,
                    curriculumContentCode,
                    curriculumContentUrl,
                    exemplarItem,
                    testLevel,
                    difficulty,
                    proficiencyLevel,
                    attempts,
                    correct,
                    incorrect,
                    notAttempted,
                    correctPercentage,
                    domainAndYearLevelAttempts,
                    attemptedPercentage,
                    parallelTestSection,
                    locationInTestSection)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     """
                     , (row["questionId"],
                        row["eventIdentifier"],
                        row["questionIdentifier"],
                        row["nodeIdentifier"],
                        row["descriptor"],
                        row["domain"],
                        row["domainId"],
                        row["subdomain"],
                        row["subdomainAbbr"],
                        row["subdomain3"],
                        row["curriculumContentCode"],
                        row["curriculumContentUrl"],
                        row["exemplarItem"],
                        row["testLevel"],
                        row["difficulty"],
                        row["proficiencyLevel"],
                        row["attempts"],
                        row["correct"],
                        row["incorrect"],
                        row["notAttempted"],
                        row["correctPercentage"],
                        row["domainAndYearLevelAttempts"],
                        row["attemptedPercentage"],
                        row["parallelTestSection"],
                        row["locationInTestSection"])
        )
    conn.commit()


def insert_students(conn, students_df):
    """
    Insert the students into the students table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    students_df: pd.DataFrame
    A DataFrame containing the student information
        
    Returns:
    None
    """
    for _, row in students_df.iterrows():
        conn.execute("""
            INSERT INTO students (studentId, studentLOTE, schoolStudentId) VALUES (?, ?, ?)
        """, (row["student.studentId"], row["student.metadata.studentLOTE"], row["student.metadata.schoolStudentId"]))
    conn.commit()


def insert_students_scores(conn, student_scores_df):
    """
    Insert the student scores into the student_scores table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    student_scores_df: pd.DataFrame
    A DataFrame containing the student scores information
        
    Returns:
    None
    """
    for _, row in student_scores_df.iterrows():
        conn.execute("""
            INSERT INTO student_scores (studentId, domainId, possibleRawScore, studentRawScore, scaledScore) VALUES (?, ?, ?, ?, ?)
        """, (row["student.studentId"], row["domain.domainId"], row["possibleRawScore"], row["studentRawScore"], row["scaledScore"]))
    conn.commit()


def insert_attempts(conn, attempts_df):
    """
    Insert the attempts into the attempts table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    attempts_df: pd.DataFrame
    A DataFrame containing the attempts information
        
    Returns:
    None
    """
    for _, row in attempts_df.iterrows():
        conn.execute("""
            INSERT INTO attempts (studentId, correct, answeredOn, questionId, questionNo, parallelTestSection, node) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (row["student.studentId"], row["correct"], row["answeredOn"], row["questionId"], row["questionNo"], row["parallelTestSection"], row["node"]))
    conn.commit()


def insert_writing_marking_scheme(conn, writing_marking_scheme_df):
    """
    Insert the writing marking scheme into the writing_marking_scheme table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    writing_marking_scheme_df: pd.DataFrame
    A DataFrame containing the writing marking scheme information
        
    Returns:
    None
    """
    for _, row in writing_marking_scheme_df.iterrows():
        conn.execute("""
            INSERT INTO writing_marking_scheme (questionId, markingSchemeId, name, description, domainId, testLevel, proficiency, scoreD, sDescription) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (row["questionId"], row["id"], row["name"], row["description"], row["domainId"], row["testLevel"], row["proficiencyLevel"],row["scoreD"], row["sDescription"]))
    conn.commit()


def insert_writing_responses(conn, writing_responses_df):
    """
    Insert the writing responses into the writing_responses table
    
    Parameters:
    conn: sqlite3.Connection
    The connection to the database
    writing_responses_df: pd.DataFrame
    A DataFrame containing the writing responses information
        
    Returns:
    None
    """
    for _, row in writing_responses_df.iterrows():
        conn.execute("""
            INSERT INTO writing_responses (studentId, writingRespose, question_id, markingSchemeId, score, testLevel) VALUES (?, ?, ?, ?, ?, ?)
        """, (row["student.studentId"], row["writingResponse"], row["questionId"], row["rowguid"], row["effectiveScore"], row["student.testLevel"]))
    conn.commit()