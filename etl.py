import os
import tempfile
import pandas as pd
import requests, zipfile
import datetime
import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import dotenv, os
from dotenv import dotenv_values
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from credentials import get_database_conn, get_accessbase_conn


def extract_from_google():
    gauth = GoogleAuth()
    
    # Create GoogleDrive instance with authenticated GoogleAuth instance.
    drive = GoogleDrive(gauth)
    
    # Initialize GoogleDriveFile instance with file id.
    file_obj = drive.CreateFile({'id': '1VyCGCAfFuEK7vB1C9Vq8iPdgBdu-LDM4'})
    file_obj.GetContentFile('PUB150.ZIP') # Download file as 'PUB150.ZIP'.

    #extract ms access file from the zip folder
    with zipfile.ZipFile('PUB150.ZIP', 'r') as zip_ref:
        zip_ref.extractall('data/')


def transform_file():
    conn = get_accessbase_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM "WPI Data"')
    data = cursor.fetchall()

    #get columns
    cursor.execute('SELECT TOP 1 * FROM "WPI Data"')
    column_names = [column[0] for column in cursor.description]

    #change data structure to be fully a list
    new_list = [list(i) for i in data]
    df = pd.DataFrame(new_list, columns=column_names)

    return df


def load_data(db_user_name, db_password, db_name, port, host):
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@localhost/{db_name}')


        # Read the transformed csv into a DataFrame
    df = transform_file()
    df.to_sql('WPI_data', con=engine, if_exists='replace', index=False)
    print('Data successfully written to postgreSQL database')





