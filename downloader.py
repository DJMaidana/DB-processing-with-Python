import os
import requests
from datetime import datetime
from decouple import config
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("info.log")
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(asctime)s] {%(levelname)s} %(name)s: %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def download_source_data():

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

    #Checks if folder path for current month exists, otherwise creates it
    if not os.path.isdir(museos_dir):
        logger.warning("Path to \"Museos\" data not found, creating path.")
        os.makedirs(museos_dir)
    #Downloads data from web and writes it to a .csv file
    r = requests.get(config("URL_MUSEOS_CSV"), allow_redirects=True)
    with open (os.path.join(museos_dir, museos_name), "wb") as csv_file:
        csv_file.write(r.content)
        logger.info("\"Museos\" data downloaded successfully.")


    if not os.path.isdir(cines_dir):
        logger.warning("Path to \"Cines\" data not found, creating path.")
        os.makedirs(cines_dir)

    r = requests.get(config("URL_CINES_CSV"), allow_redirects=True)
    with open (os.path.join(cines_dir, cines_name), "wb") as csv_file:
        csv_file.write(r.content)
        logger.info("\"Cines\" data downloaded successfully.")


    if not os.path.isdir(bibliotecas_dir):
        logger.warning("Path to \"Bibliotecas\" data not found, creating path.")
        os.makedirs(bibliotecas_dir)

    r = requests.get(config("URL_BIBLIOTECAS_CSV"), allow_redirects=True)
    with open (os.path.join(bibliotecas_dir, bibliotecas_name), "wb") as csv_file:
        csv_file.write(r.content)
        logger.info("\"Bibliotecas\" data downloaded successfully.")