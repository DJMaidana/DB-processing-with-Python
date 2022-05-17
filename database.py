from ast import Pass
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
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("info.log")
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(asctime)s] {%(levelname)s} %(name)s: %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
#############################


def create_database():

    newdb_name = config("POSTGRES_DB_NAME")
    logger.info(f"Creating Database {newdb_name}...")

    # Establishing postgres connection, uses default db
    conn = psycopg2.connect(
    database = "postgres", 
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
    # Connects to postgres
    conn = psycopg2.connect(
    database = config("POSTGRES_DB_NAME"), 
    user = config("POSTGRES_USER_NAME"), 
    password = config("POSTGRES_PASSWORD"),
    host= config("POSTGRES_HOST"),
    port= config("POSTGRES_PORT")
    )
    conn.autocommit = True
    
    cursor = conn.cursor()
    # Opens and splits the .sql file into separate commands
    file = open(config("SQL_SCRIPTS_DIR"), "r")
    sqlScript = file.read()
    sqlCommands = sqlScript.split(";")
    logger.info("Executing SQL table creation script...") 
    # Executes commands in .sql file
    for command in sqlCommands:
        try:
            table_name = command.split(" ")[2]
            cursor.execute(command)
            logger.info("Created table \"{}\" in database \"{}\" as User \"{}\"".format(table_name, config("POSTGRES_DB_NAME"), config("POSTGRES_USER_NAME")))
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

    museos_name = f"museos-{cur_day}-{cur_month_num}-{cur_year}.csv"
    cines_name = f"cines-{cur_day}-{cur_month_num}-{cur_year}.csv"
    bibliotecas_name = f"bibliotecas-{cur_day}-{cur_month_num}-{cur_year}.csv"

    museos_df = pd.read_csv(os.path.join(museos_dir, museos_name))
    cines_df = pd.read_csv(os.path.join(cines_dir, cines_name))
    bibliotecas_df = pd.read_csv(os.path.join(bibliotecas_dir, bibliotecas_name))
    
    ################################################
    ## Create a common DataFrame from the three sources
    ################################################
    # Renames some columns so all DataFrames match headers
    museos_df.columns = museos_df.columns.str.title()
    try:
        museos_df = museos_df.rename(columns={"Categoria": "Categoría", "Telefono": "Teléfono", "Direccion": "Dirección"})
    except Exception as ex:
        logger.warning(ex)
    try:
        bibliotecas_df = bibliotecas_df.rename(columns={"Domicilio": "Dirección"})
    except Exception as ex:
        logger.warning(ex)
    # Concatenates the Dataframes into a single DataFrame
    m_c_b = pd.concat([cines_df, bibliotecas_df, museos_df])
    # Copies into another DataFrame only the columns of interest
    m_c_b_columns = m_c_b[["Cod_Loc", "IdProvincia", "IdDepartamento", "Categoría", "Provincia", "Localidad", "Nombre", "Dirección", "CP", "Teléfono", "Mail", "Web"]]    
    # This is the DataFrame to be saved
    m_c_b_final = m_c_b_columns.copy()

    ################################################
    ## Create a DataFrame with cinema information
    ################################################
    cines_prov_df = pd.DataFrame(columns=["Butacas_Total", "Pantallas_Total", "Espacios_INCAA"])
    cines_df["Total"] = "Total" 
    cines_prov_df["Butacas_Total"] = cines_df.pivot_table(index="Provincia", columns="Total", values="Butacas", aggfunc="sum")["Total"]
    cines_prov_df["Pantallas_Total"] =cines_df.pivot_table(index="Provincia", columns="Total", values="Pantallas", aggfunc="sum")["Total"]

    # Counts Incaa spaces per province
    provincias_incaa = {}

    for index, row in cines_df.fillna(0).iterrows():
        # print(row["Provincia"], row["espacio_INCAA"])
        if row["Provincia"] not in provincias_incaa:
            provincias_incaa[row["Provincia"]] = 0
            if type(row["espacio_INCAA"]) ==  int and row["espacio_INCAA"] > 0:
                provincias_incaa[row["Provincia"]] = 1
            elif type(row["espacio_INCAA"]) ==  str and row["espacio_INCAA"].lower() == "si":
                provincias_incaa[row["Provincia"]] = 1
        elif row["Provincia"] in provincias_incaa:
            if type(row["espacio_INCAA"]) ==  int and row["espacio_INCAA"] > 0:
                provincias_incaa[row["Provincia"]] += 1
            elif type(row["espacio_INCAA"]) ==  str and row["espacio_INCAA"].lower() == "si":
                provincias_incaa[row["Provincia"]] += 1

    # Loads the dictionary values to the DataFrame's respective provinces
    cines_prov_df = cines_prov_df.fillna(0)
    for key, value in sorted(provincias_incaa.items()): 
        for index, row in cines_prov_df.iterrows():
            if index == key:
                row["Espacios_INCAA"] = value

    ################################################
    ## Creates DataFrame with total amount of entries
    ################################################  
    # Counts amount of entries per source file
    name_column = ["Fuente: Cines", "Fuente: Museos", "Fuente: Bibliotecas"]
    entries_list = []
    entries_list.append(len(cines_df.index))
    entries_list.append(len(museos_df.index))
    entries_list.append(len(bibliotecas_df.index))

    # Count amount of entries per category
    categories_dict = {}
    for index, row in cines_df.iterrows():
        if row["Categoría"] not in categories_dict:
            categories_dict[row["Categoría"]] = 1
        else:
            categories_dict[row["Categoría"]] += 1

    for index, row in museos_df.iterrows():
        if row["Categoría"] not in categories_dict:
            categories_dict[row["Categoría"]] = 1
        else:
            categories_dict[row["Categoría"]] += 1

    for index, row in bibliotecas_df.iterrows():
        if row["Categoría"] not in categories_dict:
            categories_dict[row["Categoría"]] = 1
        else:
            categories_dict[row["Categoría"]] += 1

    for key, value in categories_dict.items():
        name_column.append(f"Categoría: {key}")
        entries_list.append(value)

    # Counts amount of entries per province, by categories
    province_dict = {}
    
    for index, row in m_c_b_final.sort_values(by="Provincia").iterrows():
        if row["Provincia"] not in province_dict:
            province_dict[row["Provincia"]] = {}
        else:
            if row["Categoría"] not in province_dict[row["Provincia"]]:
                province_dict[row["Provincia"]][row["Categoría"]] = 1
            else:
                province_dict[row["Provincia"]][row["Categoría"]] += 1

    for prov, cats in province_dict.items():
        for cat, amount in cats.items():
            name_column.append(f"{prov}: {cat}")
            entries_list.append(amount)


    # Add to DataFrame
    total_entries = pd.DataFrame(columns=["Nombre", "Cantidad total de registros"])
    total_entries["Nombre"] = name_column
    total_entries["Cantidad total de registros"] = entries_list

    ################################################
    ## Save all DataFrames as Tables to postgres
    ################################################  
    # Connects to postgres
    pg_db_name = config("POSTGRES_DB_NAME")
    pg_username = config("POSTGRES_USER_NAME")
    pg_password = config("POSTGRES_PASSWORD")
    pg_host = config("POSTGRES_HOST")
    pg_port = config("POSTGRES_PORT")
    
    conn = create_engine(f"postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_db_name}")
    # Updates postgres table
    logger.info(f"Updating table \"museos_cines_bibliotecas\" in Database \"{pg_db_name}\" as user \"{pg_username}\"...")
    m_c_b_final.to_sql("museos_cines_bibliotecas", conn, if_exists='replace', index = False)
    logger.info("Table \"museos_cines_bibliotecas\" updated successfully.")
    logger.info(f"Updating table \"cines\" in Database \"{pg_db_name}\" as user \"{pg_username}\"...")
    cines_prov_df.to_sql("cines", conn, if_exists='replace', index = True) # Index True because the index is already the name of the Provinces
    logger.info("Table \"cines\" updated successfully.")
    logger.info(f"Updating table \"registros\" in Database \"{pg_db_name}\" as user \"{pg_username}\"...")
    total_entries.to_sql("registros", conn, if_exists="replace", index= False)
    logger.info("Table \"registros\" updated successfully.\n")
    logger.info(f"Database \"{pg_db_name}\" updated successfully as user \"{pg_username}\".")