#!/usr/bin/env python3
"""
Q3: Find products based on keywords from top recommended products' categories
"""

import psycopg2
import pandas as pd
import os
from tabulate import tabulate

# Database connection parameters
DB_NAME = 'bd_a2'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'
DB_HOST = 'localhost'
DB_PORT = '5432'

# Path to SQL file
SQL_FILE = './q3.sql'

def run_q3():
    """Execute Q3 query from .sql file and display results"""
    
    # Check if SQL file exists
    if not os.path.exists(SQL_FILE):
        print(f"ERROR: SQL file not found: {SQL_FILE}")
        return
    
    # Read query from file
    with open(SQL_FILE, 'r') as f:
        query = f.read()
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    except Exception as e:
        print(f"Database connection failed: {e}")
        return
    
    # Execute query
    try:
        df = pd.read_sql_query(query, conn)
        
        if len(df) > 0:
            # Display results table
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            df.to_csv("../output/q3_psql.csv", index=False)
            pass
        else:
            print("Query returned no results")
            pass
            
    except Exception as e:
        print(f"Query execution failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_q3()