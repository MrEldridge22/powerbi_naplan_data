import sqlite3

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
        domainId TEXT
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
        subdomain_id INTEGER,
        question TEXT,
        FOREIGN KEY (subdomain_id) REFERENCES subdomains(id)
    )
    """)
    
    # Create the attempts table
    conn.execute("""
    CREATE TABLE attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        question_id INTEGER,
        proficiency_score REAL,
        FOREIGN KEY (question_id) REFERENCES questions(id)
    )
    """)
    
    # Create the answers table
    conn.execute("""
    CREATE TABLE answers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attempt_id INTEGER,
        answer TEXT,
        FOREIGN KEY (attempt_id) REFERENCES attempts(id)
    )
    """)
    
    conn.commit()
    
    return conn