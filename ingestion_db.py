
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

import time 

# For Logging 
import logging 
logging.basicConfig(
    filename = "Logs/ingestion_db.log" , 
    level = logging.DEBUG , 
    format = "%(asctime)s - %(levelname)s - %(message)s" , 
    filemode = "a"
)

import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import create_engine

pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns',None)
pd.set_option('display.max_colwidth',None)

engine = create_engine('sqlite:///vendor_inventory.db')

def ingest_db(file_path_or_df, table_name, engine):
  """
  This function will load data into the Database.
  It accepts either a file path to a CSV or a pandas DataFrame.
  """
  if isinstance(file_path_or_df, str):
    # If input is a string (file path), read CSV in chunks
    chunksize = 100000  # Define a chunk size
    for chunk in pd.read_csv(file_path_or_df, chunksize=chunksize):
      chunk.to_sql(table_name, con=engine, if_exists='append', index=False)
  else:
    # If input is a DataFrame, write it directly to SQL
    file_path_or_df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logging.info(f"DataFrame successfully written to {table_name} table")

def load_raw_data() : 
  """
  This function will load the csv as dataframes and ingest into the DB 
  """
  start = time.time()

  for file in os.listdir('data') :
    if ".csv" in file :
      file_path = 'data/'+file
      logging.info(f"Ingesting {file} in db")
      ingest_db(file_path, file[:-4], engine)

  logging.info("------------Ingestion_Complete------------")
  end = time.time()

  total_time_taken = (end - start)/60 
  logging.info(f"Total Time Taken:{total_time_taken} mins") 

if __name__ == "__main__" : 
  load_raw_data()
