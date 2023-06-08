import psycopg2
from sqlalchemy import create_engine, text
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
    conn = psycopg2.connect(f'dbname={db_name} user={db_user_name} password={db_password} host={host} port={port}')

    return conn


def get_accessbase_conn():
    return pyodbc.connect( r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=data/access/WPI.mdb')

def engine():
    dotenv.load_dotenv(r"C:\Users\NGSL0161\Desktop\Data Engineering class\projects\environmental variables\.env")
    db_user_name = os.getenv('DB_USER_NAME')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME2')
    port = os.getenv('PORT')
    host = os.getenv('HOST')
    engine = create_engine(f'postgresql+psycopg2://{db_user_name}:{db_password}@localhost/{db_name}')
    return engine


def question():
    conn = engine()


    query1 = text("""
        SELECT "Main_port_name", "Wpi_country_code", 
		"Latitude_degrees", "Longitude_degrees",
         6371000 * acos(
         cos(radians("Latitude_degrees")) *
         cos(radians((SELECT "Latitude_degrees" FROM "WPI_data" WHERE "Main_port_name" = 'JURONG ISLAND' AND "Wpi_country_code" = 'SG'))) *
         cos(radians((SELECT "Longitude_degrees" FROM "WPI_data" WHERE "Main_port_name" = 'JURONG ISLAND' AND "Wpi_country_code" = 'SG')) - radians("Longitude_degrees")) +
         sin(radians("Latitude_degrees")) *
         sin(radians((SELECT "Latitude_degrees" FROM "WPI_data" WHERE "Main_port_name" = 'JURONG ISLAND' AND "Wpi_country_code" = 'SG')))
       ) AS distance
        FROM "WPI_data"
        WHERE "Main_port_name" != 'JURONG ISLAND' AND "Wpi_country_code" = 'SG'
        ORDER BY distance ASC
        LIMIT 5;
        """)
    
    query2 = """
        SELECT "Wpi_country_code", COUNT(*) as port_count
        FROM "WPI_data"
        WHERE "Load_offload_wharves" = 'Y'
        GROUP BY "Wpi_country_code"
        ORDER BY port_count DESC
        LIMIT 1;
        """
    
    query3= """
        SELECT "Main_port_name", 
		"Wpi_country_code", 
		"Latitude_degrees", 
		"Longitude_degrees",
        point("Longitude_degrees", "Latitude_degrees") <-> point(-38.706256, 32.610982) as distance

        FROM "WPI_data"
        WHERE "Supplies_provisions" = 'Y' AND "Supplies_water" = 'Y' AND "Supplies_fuel_oil" = 'Y' AND "Supplies_diesel_oil" = 'Y'
        ORDER BY point("Longitude_degrees", "Latitude_degrees") <-> point(-38.706256, 32.610982) ASC
        LIMIT 1;
        """



    
    # Execute the query and fetch the results
    with conn.connect() as con:
        df = pd.DataFrame(con.execute(query1))
        df.to_csv(f'data/questions/question1.csv', index=False)

    # Execute the query and fetch the results
    with conn.connect() as con:
        df = pd.DataFrame(con.execute(query2))
        df.to_csv(f'data/questions/question2.csv', index=False)

    # Execute the query and fetch the results
    with conn.connect() as con:
        df = pd.DataFrame(con.execute(query3))
        df.to_csv(f'data/questions/question3.csv', index=False)

    

def delete_files(folder):
    files = os.listdir(folder)
    for file in files:
        file_path = os.path.join(folder, file)
        os.remove(file_path)
        print(f"Deleted file: {file}")


