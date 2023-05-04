from src.log import logger
from src.exceptions import CustomException
from src.utils import load_obj, save_obj, fetch_tables_list
# from src.components.data_ingestion import DataIngestion

import os
import pandas as pd
import numpy as np


class DataTransformationConfig:
    def __init__(self):
        self.df_name_list = fetch_tables_list(schema='production')[0]
        self.preprocessor_pipeline_paths_dict = self.__prep_pipeline_paths()
        self.preprocessor_paths_dict = self.__prep_paths()

    def __prep_pipeline_paths(self):
       paths = {}
       try:
           for  df_name in self.df_name_list:
               paths[df_name] = os.path.join('.artifacts', df_name, 'preprocessor_pipeline.pkl')
               logger.info(f"Defined the preprocessor pipeline path for dataframe {df_name}.")

           return paths
       except Exception as e:
           raise CustomException(e)
       
    def __prep_paths(self):
       paths = {}
       try:
           for  df_name in self.df_name_list:
               paths[df_name] = os.path.join('.artifacts', df_name, 'preprocessor.pkl')
               logger.info(f"Defined the preprocessor object path for dataframe {df_name}.")

           return paths
       except Exception as e:
           raise CustomException(e)


class DataTransformation:
    def __init__(self):
        self.__transformation_config = DataTransformationConfig()
        logger.info(f"Data Transformation config captured.")

    def initiate_data_transformation(self, train_data_dict:dict, test_data_dict:dict, label_dict:dict):

        df_name_list = self.__transformation_config.df_name_list
        prep_pip_dict = self.__transformation_config.preprocessor_pipeline_paths_dict
        prep_dict = self.__transformation_config.preprocessor_paths_dict
        logger.info("Stored the paths to preprocessor pipeline and objects as dictionary.")

        train_arr_dict = {}
        test_arr_dict = {}

        for df_name in df_name_list:
            try:
                train_df = pd.read_csv(train_data_dict[df_name])
                test_df = pd.read_csv(test_data_dict[df_name])
                logger.info(f"Loaded the train and test data for the dataframe {df_name}.")
            except Exception as e:
                raise CustomException(e)
        
            try:
                prep_pipeline = load_obj(prep_pip_dict[df_name])
                logger.info("Loaded the preprocessor pipeline object into 'prep_pipeline' variable.")
            except Exception as e:
                raise CustomException(e)
        
            try:
                feats_train_df = train_df.drop(label_dict[df_name], axis=1)
                label_train_df = train_df[label_dict[df_name]]
                logger.info(f"Split train data in {df_name} into features and label dataframes.")
            except Exception as e:
                raise CustomException(e)

            try:
                feats_test_df = test_df.drop(label_dict[df_name], axis=1)
                label_test_df = test_df[label_dict[df_name]]
                logger.info(f"Split test data in {df_name} into features and label dataframes.")
            except Exception as e:
                raise CustomException(e)

            try:
                feats_train_arr = prep_pipeline.fit_transform(feats_train_df)
                feats_test_arr = prep_pipeline.transform(feats_test_df)
                logger.info(f"Applied the preprocessor pipeline onto the train and test features sets in {df_name}.")
            except Exception as e:
                raise CustomException(e)

            try:
                train_arr_dict[df_name] = np.c_[feats_train_arr, np.array(label_train_df)]
                test_arr_dict[df_name] = np.c_[feats_test_arr, np.array(label_test_df)]
                logger.info("Concatenated the train and test feats and label arrays.")
            except Exception as e:
                raise CustomException(e)

            try:
                save_obj(file_path=prep_dict[df_name], obj=prep_pipeline)
                logger.info(f"Preprocessor object saved into '.artifacts/{df_name}/preprocessor.pkl'")
            except Exception as e:
                raise CustomException(e)
                
        return(train_arr_dict, test_arr_dict, prep_dict)
    






