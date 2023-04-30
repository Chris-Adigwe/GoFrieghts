import psycopg2
from sqlalchemy import create_engine
import dotenv, os
from dotenv import dotenv_values
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pyodbc
import requests, zipfile

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




