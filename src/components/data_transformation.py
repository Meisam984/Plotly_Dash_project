from src.log import logger
from src.exceptions import CustomException
from src.utils import load_obj, save_obj, fetch_tables_list, add_trade_indicators, add_label
import pandas as pd
import numpy as np
import os
from src.components.data_ingestion import DataIngestion
from src.pipelines.transformation_pipeline import TransformationPipeline


class DataTransformationConfig:
    def __init__(self):
        self.df_name_list = fetch_tables_list(schema='production')
        self.df_name_list.remove("prd_exchange_news")
        self.df_name_list.remove("prd_exchange_indexvalues")
        self.preprocessor_pipeline_paths_dict = self.__prep_pipeline_paths()
        self.preprocessor_paths_dict = self.__prep_paths()

    def __prep_pipeline_paths(self):
       paths = {}
       try:
           for df_name in self.df_name_list:
               paths[df_name] = os.path.join('.artifacts', df_name, 'preprocessor_pipeline.pkl')
               logger.info(f"Defined the preprocessor pipeline path for dataframe {df_name}.")

           return paths
       except Exception as e:
           raise CustomException(e)
       
    def __prep_paths(self):
       paths = {}
       try:
           for df_name in self.df_name_list:
               paths[df_name] = os.path.join('.artifacts', df_name, 'preprocessor.pkl')
               logger.info(f"Defined the preprocessor object path for dataframe {df_name}.")

           return paths
       except Exception as e:
           raise CustomException(e)


class DataTransformation:
    def __init__(self):
        self.__transformation_config = DataTransformationConfig()
        logger.info(f"Data Transformation config captured.")


    def initiate_data_transformation(self, train_data_dict:dict, test_data_dict:dict):

        df_name_list = self.__transformation_config.df_name_list
        prep_pip_dict = self.__transformation_config.preprocessor_pipeline_paths_dict
        prep_dict = self.__transformation_config.preprocessor_paths_dict
        logger.info("Stored the paths to preprocessor pipeline and objects as dictionary.")

        TransformationPipeline().initiate_transformation_pipeline()

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
                train_df_with_indices, test_df_with_indices = add_trade_indicators(train_df, test_df, df_name)
                train_df_with_indices_labeled, test_df_with_indices_labeled = add_label(train_df_with_indices, test_df_with_indices, df_name)
            except Exception as e:
                raise CustomException(e)

            try:
                prep_pipeline = load_obj(prep_pip_dict[df_name])
                logger.info("Loaded the preprocessor pipeline object into 'prep_pipeline' variable.")
            except Exception as e:
                raise CustomException(e)

            try:
                X_train = train_df_with_indices_labeled.drop(columns='label')
                y_train = train_df_with_indices_labeled['label']
                logger.info(f"Split train data in {df_name} into features and label dataframes.")
            except Exception as e:
                raise CustomException(e)

            try:
                X_test = test_df_with_indices_labeled.drop(columns='label')
                y_test = test_df_with_indices_labeled['label']
                logger.info(f"Split test data in {df_name} into features and label dataframes.")
            except Exception as e:
                raise CustomException(e)

            try:
                X_train_arr = prep_pipeline.fit_transform(X_train)
                X_test_arr = prep_pipeline.transform(X_test)
                logger.info(f"Applied the preprocessor pipeline onto the train and test features sets in {df_name}.")
            except Exception as e:
                raise CustomException(e)

            try:
                train_arr_dict[df_name] = np.c_[X_train_arr, np.array(y_train)]
                test_arr_dict[df_name] = np.c_[X_test_arr, np.array(y_test)]
                logger.info("Concatenated the train and test feats and label arrays.")
            except Exception as e:
                raise CustomException(e)

            try:
                save_obj(file_path=prep_dict[df_name], obj=prep_pipeline)
                logger.info(f"Preprocessor object saved into '.artifacts/{df_name}/preprocessor.pkl'")
            except Exception as e:
                raise CustomException(e)
                
        return(train_arr_dict, test_arr_dict, prep_dict)
    

ingested_data = DataIngestion().initiate_data_ingestion()
DataTransformation().initiate_data_transformation(ingested_data[0], ingested_data[1])




