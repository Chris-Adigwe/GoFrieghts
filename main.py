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
from utils import get_database_conn, get_accessbase_conn, question
from etl import transform_file, load_data, extract_from_google


def main():
    
    conn = get_database_conn()
    extract_from_google()
    transform_file()
    load_data()
    question()


# Execute the script by invoking the main method
main()