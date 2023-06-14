import os
from src.exceptions import CustomException
from src.log import logger
from src.utils import fetch_tables_list, fetch_tables_dict
from sklearn.model_selection import train_test_split


# Data ingestion config class
class DataIngestionConfig:
    def __init__(self):
        self.df_name_list = fetch_tables_list(schema='production')
        self.df_dict = fetch_tables_dict(self.df_name_list, 'production')
        self.data_paths_dict = self.__data_paths()
        
    def __data_paths(self):
        paths = {}
        try:
            for i in ['raw', 'train', 'test']:
                paths[i] = {}
                for df_name in self.df_name_list:
                    paths[i][df_name] = os.path.join('.artifacts', df_name, str(f"{i}.csv"))
                    logger.info(f"Defined the data path for '{df_name}/{i}.csv'.")
        
            return paths
        except Exception as e:
            raise CustomException(e)


# Data ingestion class
class DataIngestion:
    def __init__(self):
        self.__ingestion_config = DataIngestionConfig()
        logger.info(f"Data ingestion config captured.")
    
    def initiate_data_ingestion(self):
        raw_data_dict = self.__ingestion_config.data_paths_dict['raw']
        train_data_dict = self.__ingestion_config.data_paths_dict['train']
        test_data_dict = self.__ingestion_config.data_paths_dict['test']
        logger.info(f"Data ingestion process initiated for the dataframe by storing the raw, train and test data path lists.")

        df_name_list = self.__ingestion_config.df_name_list
        df_dict = self.__ingestion_config.df_dict
        logger.info("Stored table name list and tables dictionary into 'df_name_list' and 'df_dict'.")
                        
        
        for df_name in df_name_list:
            try:
                os.makedirs(f'.artifacts/{df_name}', exist_ok=True)
                logger.info(f"Created '.artifacts' directory and '{df_name}' subdirectory.")
            except Exception as e:
                raise CustomException(e)
            
            try:
                df = df_dict[df_name]
                raw_path = raw_data_dict[df_name]
                logger.info(f"Fetched the dataframe and its storage path for {df_name}.")
            except Exception as e:
                raise CustomException(e)
            
            try:
                df.to_csv(raw_path, header=True, index=False)
                logger.info(f"Dataframe {df_name} was stored into '.artifacts/{df_name}/raw.csv' file.")
            except Exception as e:
                raise CustomException(e)
    
            try:
                train_set, test_set = train_test_split(df, test_size=0.2, random_state=102)
                logger.info(f"Dataframe {df_name} was split into train and test sets.")
            except Exception as e:
                raise CustomException(e)
    
            try:
                train_set.to_csv(train_data_dict[df_name], header=True, index=False)
                test_set.to_csv(test_data_dict[df_name], header=True, index=False)
                logger.info(f"Stored train and test data in dataframe {df_name} into '.artifacts/{df_name}/train.csv' and '.artifacts/{df_name}/test.csv', respectively.")
            except Exception as e:
                raise CustomException(e)
                
        return [train_data_dict, test_data_dict]

