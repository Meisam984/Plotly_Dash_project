from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.log import logger
from src.exceptions import CustomException
from src.utils import save_obj, fetch_tables_list

import os
import pandas as pd


class TransformationPipelineConfig:

    def __init__(self):
        self.df_name_list = fetch_tables_list(schema='production')
        self.df_paths_dict = self.__data_paths()
        self.preprocessor_pipeline_paths_dict = self.__prep_pipeline_paths()

    def __prep_pipeline_paths(self):
       paths = {}
       try:
           for df_name in self.df_name_list:
               paths[df_name] = os.path.join('.artifacts', df_name, 'preprocessor_pipeline.pkl')
               logger.info(f"Defined the preprocessor pipeline path for dataframe {df_name}.")

           return paths
       except Exception as e:
           raise CustomException(e)
       
    def __data_paths(self):
        paths = {}
        try:
            for df_name in self.df_name_list:
                paths[df_name] = os.path.join('.artifacts', df_name, 'raw.csv')
                logger.info(f"Defined the raw data path for dataframe {df_name}.")

            return paths
        except Exception as e:
            raise CustomException(e)


class TransformationPipeline:
    def __init__(self):
        self.__transformation_pipeline_config = TransformationPipelineConfig()
        logger.info("Transformation pipeline config captured.")

        
    def initiate_transformation_pipeline(self):
        df_name_list = self.__transformation_pipeline_config.df_name_list
        df_paths_dict = self.__transformation_pipeline_config.df_paths_dict
        prep_pip_paths_dict = self.__transformation_pipeline_config.preprocessor_pipeline_paths_dict

        for df_name in df_name_list:
            
            df = pd.read_csv(df_paths_dict[df_name])
            logger.info(f"Stored the dataframe df, reading through '.artifacts/{df_name}/raw.csv'.")

            try:
                for col in df.columns.values.tolist():
                    if any([x in col for x in ['id', 'meta']]):
                        df_ind = df_ind.astype({col: str})
                        logger.info(f"Converted the column, {col}, type into string.")
                    if col in ['category.id', 'market.id']:
                        df_ind[col] = df_ind[col].astype('category')
                        logger.info(f"Converted the column, {col}, type into category.")

                print(df.info())
            except Exception as e:
                raise CustomException(e)

            cat_feats = df.select_dtypes(include=['category']).columns.to_list()
            logger.info("Stored the categorical features into the list 'cat_feats'.")
            print(cat_feats)

            num_feats = df.select_dtypes(include=['number']).columns.to_list()
            logger.info("Stored the numerical features into the list 'num_feats'.")
            print(num_feats)

            try:
                num_pipeline = Pipeline(steps=[('imputer', SimpleImputer(strategy='median')),
                                               ('scalar', StandardScaler())])
                logger.info("Numerical pipeline created as a series of SimpleImputer and StandardScalar.")
            except Exception as e:
                raise CustomException(e)
            
            try:
                cat_pipeline = Pipeline(steps=[('imputer', SimpleImputer(strategy='most_frequent')),
                                               ('one_hot_encoder', OneHotEncoder()),
                                               ('scalar', StandardScaler(with_mean=False))])
                logger.info("Categorical pipeline created as a series of SimpleImputer, OneHotEncoder and StandardScalar.")
            except Exception as e:
                raise CustomException(e)
            
            try:
                preprocessor_pipeline = ColumnTransformer([('num_pipeline', num_pipeline, num_feats),
                                                           ('cat_pipeline', cat_pipeline, cat_feats)])
                logger.info("Created the preprocessor object as a series of numerical and categorical pipelines.")
            except Exception as e:
                raise CustomException(e)
            
            try:
                save_obj(file_path=prep_pip_paths_dict[df_name], obj=preprocessor_pipeline)
                logger.info(f"Saved the preprocessor pipeline object into '.artifacts/{df_name}/preprocessor_pipeline.pkl' file.")
            except Exception as e:
                raise CustomException(e)
        




