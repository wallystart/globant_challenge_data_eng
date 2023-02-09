import os
import logging
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col,date_format
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

'''
Title: avro2postgres
Description: This code is responsible for importing avro files into a PostgreSQL database in a simple way.
'''

def init_spark():
    """
    Initializes SparkSession and returns SparkSession and SparkContext objects
    
    Returns:
        SparkSession, SparkContext: objects of SparkSession and SparkContext respectively
    """
    logging.info("Initializing Spark session")
    sql = SparkSession.builder\
        .appName("avro2postgres")\
        .config("spark.jars", "/opt/workspace/spark-apps/packages/postgresql-42.2.22.jar,/opt/workspace/spark-apps/packages/spark-avro_2.12-3.0.2.jar")\
        .getOrCreate()
    sc = sql.sparkContext
    logging.info("Spark session initialized successfully")
    return sql, sc

def main():
    """
    Main function for processing and storing avro files in postgreSQL database
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
    tables = ["departments", "jobs", "hired_employees"]
    schema = "dbo"
    file_extension = "avro"
    
    # Initializing connections
    sql, _ = init_spark()
    engine = create_engine("postgresql://globant_super_admin:pass1234@postgres_database:5432/globant_challenge_db")
    metadata = MetaData(bind=engine)

    # Getting table names
    if tables == "*":
        tables = next(os.walk(root_avro_path))[1]

    logging.info(f"Tables to ingest {tables}")
    for table_name in tables:
        full_path = os.path.join(root_avro_path, table_name, f"{table_name}.{file_extension}")
        exists_pg_table = table_name in engine.table_names(schema=schema)
        
        # Perfom read
        logging.info(f"Reading data from {full_path}")
        df = sql.read.load(full_path, format="avro")
        df = df.drop("id")

        # TODO: Automatic mapping to datatypes
        if not exists_pg_table:
            # Create the table if it does not exist
            logging.info(f"Table {schema}.{table_name} does not exist. Creating table")
            Table(table_name, metadata,
                  Column("id", Integer, primary_key=True),
                *[Column(c, String(60)) for c in df.columns]
            ).create()
        else:
            # Truncate the table if it exists
            logging.info(f"Table {schema}.{table_name} exists. Truncating table")
            engine.execute(f"TRUNCATE TABLE {schema}.{table_name} RESTART identity CASCADE")

        # Perform write in append mode
        logging.info(f"Writing data to {schema}.{table_name}")
        df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"{schema}.{table_name}") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .mode("append") \
            .save()

    logging.info(f"Finish")
  
if __name__ == '__main__':
    main()