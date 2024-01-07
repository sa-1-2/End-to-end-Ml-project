import zipfile
import os
import urllib.request as request
from mlproject import logger
from mlproject.utils.common import get_size
from mlproject.entity.config_entity import DataIngestionConfig
from pathlib import Path

# constructing class DataIngestion for downloading the data & unzipping the file
class DataIngestion:
    # constructor containg config variable with class name DataIngestionConfig
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    # function download_file will download the file from url
    def download_file(self):
        if not os.path.exists(self.config.local_data_file):
            filename, header = request.urlretrieve(url= self.config.source_URL, filename=self.config.local_data_file)
            logger.info("f{filename} download! with following file info: \n{header}")
        else:
            logger.info(f"File already exists of size:{get_size(Path(self.config.local_data_file))}")
        
    # function extract_zip_file will extract zip file into directory
    def extract_zip_file(self):
        """
        zip_file_path: str
        Extracts the zip file into the data directory
        Function returns None
        """
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(file=self.config.local_data_file, mode='r') as zip_ref:
            zip_ref.extractall(unzip_path)