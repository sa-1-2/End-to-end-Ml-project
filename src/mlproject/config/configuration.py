from mlproject.constants import *
from mlproject.utils.common import read_yaml, create_directories
from mlproject.entity.config_entity import DataIngestionConfig

# creating class Configuration Manager for data Ingestion with making directiories,
#reading yaml files
class ConfigurationManager:
    #constants dir contains path of config, paarams, schema
    def __init__(self, config_pathfile = CONFIG_PATH_FILE, schema_pathfile = SCHEMA_PATH_FILE, 
                 params_pathfile = PARMAS_PATH_FILE):      
            
        # read_yaml function read the yaml file & return it into dictionary using ConfigBox
        self.config = read_yaml(config_pathfile) 
        self.params = read_yaml(params_pathfile)
        self.schema = read_yaml(schema_pathfile)

        # create_directories will create directory inside config.yaml under key artifacts_root
        create_directories([self.config.artifacts_root])
    
    # function get_data_ingestion_config will values under key data_ingestion
    # & create directiory & also return the values in function DataIngestionConfig
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion

        create_directories([config.root_dir])
        data_ingestion_config = DataIngestionConfig(
            root_dir=config.root_dir,
            source_URL= config.source_URL,
            local_data_file= config.local_data_file,
            unzip_dir= config.unzip_dir
        )
        return data_ingestion_config