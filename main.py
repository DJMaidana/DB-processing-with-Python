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

from downloader import download_source_data

logger.info("\nStarting source files download...\n")

download_source_data()
