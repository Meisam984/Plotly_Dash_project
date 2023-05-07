from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv
from src.exceptions import CustomException
from src.log import logger
import pandas as pd
import dill
import os
from typing import List, Any
from persiantools.jdatetime import JalaliDate

_ = load_dotenv(find_dotenv())

# Create a Postgresql connection engine, given a database
def postgres_connect():
    try:
        conn = os.getenv('DATABASE_URL')
        logger.info("Database URL fetched.")

        engine = create_engine(conn)
        logger.info("Engine created, establishing a connection to the Postgresql database, 'mabna' database.")
        return engine
    except Exception as e:
        raise CustomException(e)


# Grab the schema and return the list of all tables in that schema and the schema in a tuple.
def fetch_tables_list(schema: str):
    engine = postgres_connect()
    sql_all_tables = text(f"""SELECT table_name 
                              FROM information_schema.tables 
                              WHERE table_schema='{schema}'
                           """)
    tables_list = []
    try:
        with engine.connect() as conn:
            result = conn.execute(sql_all_tables)
            logger.info("Executed the query to grab all the table names.")
        
        for r in result:
            if all([x not in r[0] for x in ['energy', 'property']]):
                tables_list.append(r[0])
            logger.info(f"Added table name {r[0]} to tables list.")
        return (tables_list, schema)
    except Exception as e:
        raise CustomException(e)
    

# Grab the list of table names and the schema and return a dictionary of the tables and corresponding dataframes
def fetch_tables_dict(tables_list: List[str], schema:str):
    engine = postgres_connect()
    df_dict = {}

    for table in tables_list:
        sql_ind_table = text(f"""
                                 SELECT *
                                 FROM "{schema}".{table}
                              """)
        try:
            df_ind = pd.read_sql_query(sql=sql_ind_table, con=engine)
            logger.info(f"Executed the query to store {table} content into a dataframe.")
        except Exception as e:
            raise CustomException(e)
        
        try:
            df_ind.insert(loc=1, column='jal_date', value=jalali_str_to_greg(df_ind['j_date'])['jalali'])                    
            df_ind.insert(loc=2, column='greg_date', value=jalali_str_to_greg(df_ind['j_date'])['gregorian'])
            df_ind.drop(columns=['j_date'], inplace=True)
            logger.info(f"Added the 'jal_date' and 'greg_date' columns to the dataframe {table} as datetime type.")

        except Exception as e:
            raise CustomException(e)
        
        try:
            for col in df_ind.columns.values.tolist():
                if any([x in col for x in ['id', 'meta']]):
                    df_ind = df_ind.astype({col: str})
                    logger.info(f"Converted the column, {col}, type into string.")
            
            df_dict[table] = df_ind
            logger.info(f"Added table {table} to the df_dict object.")
        except Exception as e:
            raise CustomException(e)
    
    return df_dict


# Store obj into a file
def save_obj(file_path:str, obj: Any):
    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Created the '{dir_path}' directory.")
    except Exception as e:
        raise CustomException(e)
    
    try:
        with open(file_path, "wb") as f:
            dill.dump(obj, f)
            logger.info(f"Stored the object {type(obj)} into '{file_path}'.")
    except Exception as e:
        raise CustomException(e)
    

# Load an object from a file
def load_obj(file_path:str):
    try:
        with open(file_path, "rb") as f:
            obj = dill.load(f)
            logger.info(f"Loaded the file path, {file_path} content.")
            return obj
    except Exception as e:
        raise CustomException(e)
    

# Convert string date to Jalali and Gregorian dates in a dictionary
def jalali_str_to_greg(date_str_series):
    date_str_df = date_str_series.str.split('/', expand=True)
    year_series = date_str_df[0].astype(int)
    month_series = date_str_df[1].astype(int)
    day_series = date_str_df[2].astype(int)

    j_dates = []
    g_dates = []
    for i in year_series.index:
        j_dates.append(JalaliDate(year_series[i], month_series[i], day_series[i]))
        g_dates.append(JalaliDate(year_series[i], month_series[i], day_series[i]).to_gregorian())

    j_date_series = pd.Series(j_dates)
    g_date_series = pd.Series(g_dates)
    dict_date_series = dict(jalali=j_date_series, gregorian=g_date_series)
    return dict_date_series

