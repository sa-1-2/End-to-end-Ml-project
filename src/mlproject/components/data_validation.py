from mlproject import logger
import pandas as pd
import os
from mlproject.entity.config_entity import DataValidationConfig


class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config
    
    def validate_all_columns(self) -> bool:
        try:
            validation_status = None

            data = pd.read_csv(self.config.unzip_data_dir)
            all_columns = data.columns

            all_schema = self.config.all_schema.keys()
            for cols in all_columns:
                if cols not in all_schema:
                    validation_status= False
                    with open(self.config.STATUS_FILE, 'a') as f:
                        f.write(f"Validation Status for column: {cols} is {validation_status}\n")
            
                else:
                    validation_status = True
                    with open(self.config.STATUS_FILE, 'a') as f:
                        f.write(f"Validation Status for column: {cols} is {validation_status}\n")
                
            return validation_status
        
        except Exception as e:
            raise e

    def validate_all_dtypes(self) -> bool:
        try:
            validation_status = None

            data = pd.read_csv(self.config.unzip_data_dir)
            all_columns = data.columns
            data_type = data.dtypes

            all_schema = self.config.all_schema
            for cols in all_columns:
                if data_type[cols].name == all_schema[cols]:
                    validation_status = True
                    with open(self.config.STATUS_FILE, 'a') as f:
                        f.write(f"Validation Status for column: {cols} with datatype: {data_type[cols]} is {validation_status}\n")
                    
                else:
                    validation_status = False
                    with open(self.config.STATUS_FILE, 'a') as f:
                        f.write(f"Validation Status for column: {cols} with datatype: {data_type[cols]} is {validation_status}\n")
                        
            return validation_status
        
        except Exception as e:
            raise e