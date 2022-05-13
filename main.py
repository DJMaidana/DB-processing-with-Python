import logging
import sys
from downloader import download_source_data
from database import create_database, create_tables, process_csv

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

logger.info("\nStarting source files download...\n")
#############################

# Download and store cultural information by download date
#download_source_data()

# Create and populate DB with processed data
#create_database()
#create_tables()
process_csv()
#populate_database()