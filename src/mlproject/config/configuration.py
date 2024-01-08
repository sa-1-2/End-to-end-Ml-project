from mlproject.constants import *
from mlproject.utils.common import read_yaml, create_directories
from mlproject.entity.config_entity import DataIngestionConfig
from mlproject.entity.config_entity import DataValidationConfig
from mlproject.entity.config_entity import DataTransformationConfig
from mlproject.entity.config_entity import ModelTrainerConfig

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
        
    def get_data_validation_config(self) -> DataValidationConfig:
        config = self.config.data_validation
        schema = self.schema.COLUMNS
        create_directories([config.root_dir])
        
        data_validation_config = DataValidationConfig(
            root_dir=config.root_dir,
            unzip_data_dir= config.unzip_data_dir,
            STATUS_FILE= config.STATUS_FILE,
            all_schema=schema
        )
        return data_validation_config
    
    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation

        create_directories([config.root_dir])
        data_transformation_config = DataTransformationConfig(
            root_dir=config.root_dir,
            data_path= config.data_path,
        )
        return data_transformation_config
    
    def get_model_trainer_config(self) -> ModelTrainerConfig:
        config = self.config.model_trainer
        params = self.params.ElasticNet
        schema = self.schema.TARGET_COLUMN

        create_directories([config.root_dir])
        model_trainer_config = ModelTrainerConfig(
            root_dir = config.root_dir,
            train_data_path = config.train_data_path,
            test_data_path= config.test_data_path,
            model_name = config.model_name,
            alpha = params.alpha,
            l1_ratio = params.l1_ratio,
            target_column = schema.name     
            )
        return model_trainer_config