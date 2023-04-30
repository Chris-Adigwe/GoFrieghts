import os
import tempfile
import pandas as pd
import requests
import datetime
import psycopg2
import os
from sqlalchemy import create_engine
from sqlalchemy import text
import dotenv, os
from dotenv import dotenv_values
from credentials import get_database_conn, get_accessbase_conn
from etl import transform_file, load_data, extract_from_google


def main():
    conn = get_accessbase_conn()
    db_user_name, db_password, db_name, port, host = get_database_conn()
    # Determin if there is a scrapped data. If this is True, the data is transformed and loaded.
    # If data does not exist, the data is extracted, transformed and loaded.
    if os.path.exists('GoFrieghts/data/WPI.mdb'):
        transform_file()
        load_data(db_user_name, db_password, db_name, port, host)
    else:
        extract_from_google()
        transform_file()
        load_data(db_user_name, db_password, db_name, port, host)


# Execute the script by invoking the main method
main()