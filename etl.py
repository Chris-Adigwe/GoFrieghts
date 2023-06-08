import pandas as pd
import requests, zipfile,datetime, psycopg2, os, tempfile, dotenv, gdown
from sqlalchemy import create_engine
from sqlalchemy import text
import glob
from dotenv import dotenv_values
from utils import get_database_conn, get_accessbase_conn, engine, delete_files


def extract_from_google():
    
    file_url = 'https://drive.google.com/uc?id=1VyCGCAfFuEK7vB1C9Vq8iPdgBdu-LDM4'
    output_path = 'data/zip/file.zip'
    gdown.download(file_url, output_path)


    #extract ms access file from the zip folder
    with zipfile.ZipFile('data/zip/file.zip', 'r') as zip_ref:
        zip_ref.extractall('data/access')

    delete_files('data/zip')
    


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
    df.to_csv('data/transform/data.csv', sep=',', index=False)




def load_data():
    conn = engine()
    # Read the transformed csv into a DataFrame
    df = pd.read_csv(glob.glob('data/transform/' + '/*.csv')[0])
    df.to_sql('WPI_data', con=conn, if_exists='replace', index=False)
    print('Data successfully written to postgreSQL database')
    delete_files('data/transform')





