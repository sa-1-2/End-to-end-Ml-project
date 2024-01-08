from mlproject.config.configuration import ConfigurationManager
from mlproject.components.data_transformation import DataTransformation
from mlproject import logger 
from pathlib import Path

STAGE_NAME = "Data Transformation stage"

class DataTransformationTrainingPipeline:
    def __init__(self):
        pass

    def main(self):

        try:
            status = []
            with open(Path("artifacts/data_validation/status.txt"), 'r') as f:
                for i in f.readlines():
                    status.append(i.split(" ")[-1].strip())
            status = set(status)
            len_status = len(status)
            
            if len_status==1:
                config = ConfigurationManager()
                data_transformation_config = config.get_data_transformation_config()
                data_transformation = DataTransformation(config=data_transformation_config)
                data_transformation.train_test_splitting()
                
            else:
                raise Exception("Your data schema is not valid")
        except Exception as e:
            print(e)




if __name__ == "__main__":
    try:
        logger.info(f">>>>> stage {STAGE_NAME} started <<<<<")
        obj = DataTransformationTrainingPipeline()
        obj.main()
        logger.info(f">>>>> stage {STAGE_NAME} completed <<<<<")
    except Exception as e:
        logger.exception(e)
        raise e
