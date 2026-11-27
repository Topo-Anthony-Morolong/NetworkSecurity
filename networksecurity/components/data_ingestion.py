from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

## Configuration of the data ingestion config

from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact

import os
import sys
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split


import importlib

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGODB_URL")

try:
    AsyncMongoClient = getattr(importlib.import_module("pymongo"), "AsyncMongoClient")
except (ImportError, AttributeError):
    raise ImportError(
        "Missing MongoDB dependency. Install PyMongo with: pip install 'pymongo>=4.13'"
    )

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:
            raise NetworkSecurityException(e,sys)

    async def export_collection_as_dataframe(self):
        '''Read data from mongodb'''
        try:
            database_name= self.data_ingestion_config.database_name
            collection_name= self.data_ingestion_config.collection_name
            if not MONGO_DB_URL:
                raise ValueError("MONGODB_URL environment variable is not set")

            async with AsyncMongoClient(MONGO_DB_URL) as mongo_client:
                collection = mongo_client[database_name][collection_name]
                documents = [document async for document in collection.find()]

            df = pd.DataFrame(documents)
            if '_id' in df.columns.to_list():
                df=df.drop(columns=['_id'], axis=1)

            df.replace({'na': np.nan}, inplace=True)
            return df    
        except Exception as e:
            raise NetworkSecurityException(e, sys)


    def export_data_into_feature_store(self,dataframe: pd.DataFrame):

        try:
            feature_store_file_path= self.data_ingestion_config.feature_store_file_path
            #Creating folder

            dir_path= os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe
        
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        try:
            train_set, test_set = train_test_split(
                dataframe, test_size=self.data_ingestion_config.train_test_split_ratio                
            )
            logging.info('Performed train test split on the dataframe')

            logging.info('Exited split_data_as_train_test methods of Data_Ingestion class')

            dir_path= os.path.dirname(self.data_ingestion_config.training_file_path)

            os.makedirs(dir_path, exist_ok=True)

            logging.info(f'Exporting train and test file path.')

            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )

            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )

            logging.info(f'Exported train and test file path.')

        except Exception as e:
            raise NetworkSecurityException(e, sys)


    async def initiate_data_ingestion(self):
        try:
            dataframe = await self.export_collection_as_dataframe()
            dataframe= self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)
            Data_ingestion_artifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                            test_file_path=self.data_ingestion_config.testing_file_path)
            return Data_ingestion_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys) from e