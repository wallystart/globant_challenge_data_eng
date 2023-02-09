import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col,date_format
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

'''
Title: postgres2avro
Description: This code is responsible for importing postgres tables into avro files in a simple way.
'''

def init_spark():
    """
    Initializes SparkSession and returns SparkSession and SparkContext objects
    
    Returns:
        SparkSession, SparkContext: objects of SparkSession and SparkContext respectively
    """
    logging.info("Initializing Spark session")
    spark = SparkSession.builder\
        .appName("postgres2avro")\
        .config("spark.jars", "/opt/workspace/spark-apps/packages/postgresql-42.2.22.jar,/opt/workspace/spark-apps/packages/spark-avro_2.12-3.0.2.jar")\
        .getOrCreate()
    logging.info("Spark session initialized successfully")
    return spark

def main():
    """
    Main function for processing and storing postgreSQL database in avro files
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    # Connection configuration
    url = "jdbc:postgresql://postgres_database:5432/globant_challenge_db"
    properties = {
        "user": "globant_super_admin",
        "password": "pass1234",
        "driver": "org.postgresql.Driver"
    }
    # Path configuration
    root_avro_path = "/opt/workspace/data/backups"
    tables = "*" # ["departments", "hired_employees", "jobs"]
    schema = 'dbo'
    file_extension = "avro"
    
    # Initializing connections
    spark = init_spark()
    engine = create_engine("postgresql://globant_super_admin:pass1234@postgres_database:5432/globant_challenge_db")
    metadata = MetaData(bind=engine)

    # Getting table names
    if tables == "*":
        tables = engine.table_names(schema=schema)

    logging.info(f"Tables to backup {tables}")
    for table_name in tables:
        full_path = os.path.join(root_avro_path, table_name, f"{table_name}.{file_extension}")
        logging.info(f"Processing {schema}.{table_name} with path '{full_path}'")
        # Rename file if exists
        if os.path.exists(full_path):
            current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
            logging.info(f"Renaming existing avro file '{table_name}.{file_extension}' to '{table_name}_{current_datetime}.{file_extension}'")
            new_file_path = os.path.join(root_avro_path, table_name, f"{table_name}_{current_datetime}.{file_extension}")
            os.rename(full_path, new_file_path)
        
        # Perfom read
        logging.info(f"Reading data from {schema}.{table_name}")
        df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("driver", properties["driver"]) \
                .option("dbtable", f"{schema}.{table_name}") \
                .option("user", properties["user"]) \
                .option("password", properties["password"]) \
                .load()

        # Perform write
        logging.info(f"Writing data to {full_path}")
        df.write.format("avro").save(full_path)

    logging.info(f"Finish")
  
if __name__ == '__main__':
    main()