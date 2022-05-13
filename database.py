import psycopg2
import pandas as pd
import csv
from datetime import datetime
from sqlalchemy import create_engine
import logging
import sys
import os
from decouple import config

#############################
# Setting up logging tool
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("info.log")
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(asctime)s] {%(levelname)s} %(name)s: %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
#############################


def create_database():

    newdb_name = config("NEW_DB_NAME")
    logger.info(f"Creating Database {newdb_name}...")

    # Establishing postgres connection
    conn = psycopg2.connect(
    database = config("POSTGRES_DATABASE_NAME"), 
    user = config("POSTGRES_USER_NAME"), 
    password = config("POSTGRES_PASSWORD"),
    host= config("POSTGRES_HOST"),
    port= config("POSTGRES_PORT")
    )
    conn.autocommit = True

    cursor = conn.cursor()

    # Creates a database if it doesn't exist
    cursor.execute("SELECT datname FROM pg_database;")
    db_list = cursor.fetchall()

    if ("alkemychallenge",) not in db_list:
        sql = f'''CREATE database {newdb_name}''';
        cursor.execute(sql)
        logger.info("Database created successfully.")
        conn.close()

    else:
        logger.info(f"\nDatabase {newdb_name} already exists. Skipping DB creation...\n")
        conn.close()

def create_tables():

    conn = psycopg2.connect(
    database = config("POSTGRES_DATABASE_NAME"), 
    user = config("POSTGRES_USER_NAME"), 
    password = config("POSTGRES_PASSWORD"),
    host= config("POSTGRES_HOST"),
    port= config("POSTGRES_PORT")
    )
    conn.autocommit = True
    
    # Create an engine instance
    db = create_engine(f'postgresql+psycopg2://{config("POSTGRES_USER_NAME")}:@{config("POSTGRES_HOST")}', pool_recycle=3600)
    cursor = conn.cursor()
    # Connect to PostgreSQL server
    dbConnection = db.connect()

    file = open(config("SQL_SCRIPTS_DIR"), "r")
    sqlScript = file.read()
    sqlCommands = sqlScript.split(";")
    logger.info("Executing SQL table creation script...") 
    for command in sqlCommands:
        try:
            table_name = command.split(" ")[2]
            cursor.execute(command)
            logger.info("Created table \"{}\" in database \"{}\", User \"{}\"".format(table_name, config("POSTGRES_DATABASE_NAME"), config("POSTGRES_USER_NAME")))
        except Exception as ex:
            logger.info(f"Skipping table creation because: {ex}")

    # Close the database connection
    conn.close()

def process_csv():

    cur_year = datetime.strftime(datetime.now(), "%Y")
    cur_month = datetime.strftime(datetime.now(), "%B")
    cur_month_num = datetime.strftime(datetime.now(), "%m")
    cur_day = datetime.strftime(datetime.now(), "%d")

    museos_dir = f"Data/museos/{cur_year}-{cur_month}"
    cines_dir = f"Data/cines/{cur_year}-{cur_month}"
    bibliotecas_dir = f"Data/bibliotecas/{cur_year}-{cur_month}"
    # change this on release
    # museos_name = f"museos-{cur_day}-{cur_month_num}-{cur_year}.csv"
    # cines_name = f"cines-{cur_day}-{cur_month_num}-{cur_year}.csv"
    # bibliotecas_name = f"bibliotecas-{cur_day}-{cur_month_num}-{cur_year}.csv"
    museos_name = "museos-10-05-2022.csv"
    cines_name = "cines-10-05-2022.csv"
    bibliotecas_name = "bibliotecas-10-05-2022.csv"

    museos_df = pd.read_csv(os.path.join(museos_dir, museos_name))
    cines_df = pd.read_csv(os.path.join(cines_dir, cines_name))
    bibliotecas_df = pd.read_csv(os.path.join(bibliotecas_dir, bibliotecas_name))
    # Renames some columns on museum information so they match with the other two DataFrames
    museos_df.columns = museos_df.columns.str.title()
    museos_df = museos_df.rename(columns={"Categoria": "Categoría", "Telefono": "Teléfono"})

    m_c_b = pd.concat([cines_df, bibliotecas_df, museos_df])

    #m_c_b.to_csv("testhello.csv", index=False)

    for row in m_c_b:
        print(row)
        ## for this I'll need to pivot the table on the province values, I'll continue tomorrow
    

