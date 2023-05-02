import psycopg2
from sqlalchemy import create_engine
import dotenv, os
from dotenv import dotenv_values
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pyodbc
import requests, zipfile
import pandas as pd
import psycopg2

def get_database_conn():
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME2')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    return db_user_name, db_password, db_name, port, host

def get_accessbase_conn():
    return pyodbc.connect( r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=data/WPI.mdb')


def question(query,question):
    db_user_name, db_password, db_name, port, host = get_database_conn()
    
    conn = psycopg2.connect(f'dbname={db_name} user={db_user_name} password={db_password} host={host} port={port}')
    
    # Execute the query and fetch the results
    results = pd.read_sql_query(query, conn)
    results.to_csv(f'data/question_{question}.csv', index=False)
    
    # Close the database connection
    conn.close()








