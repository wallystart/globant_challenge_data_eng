import os
import logging
from pyspark.sql import SparkSession
#from pyspark.sql.functions import col,date_format
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

'''
Title: csv2postgres
Description: This code is responsible for importing CSV files into a PostgreSQL database in a simple way.

- The init_spark function is responsible for initializing a Spark session and returning an instance of SparkSession and SparkContext.
- In the main function, the connection configuration to the PostgreSQL database and the configuration of the path where the CSV files to be imported are located.
- Connections to Spark and the PostgreSQL database are initialized with the init_spark and create_engine functions, respectively.
- For each table in tables_to_ingest, the corresponding CSV file is read and a drop of the id column is performed on the resulting DataFrame.
- If the table does not exist in the database, the SQLAlchemy library is used to create it with a schema based on the resulting DataFrame and an auto-incrementing id column.
- Finally, the content of the DataFrame is written to the table in append mode.
'''

def init_spark():
    """
    Initializes SparkSession and returns SparkSession and SparkContext objects
    
    Returns:
        SparkSession, SparkContext: objects of SparkSession and SparkContext respectively
    """
    logging.info("Initializing Spark session")
    sql = SparkSession.builder\
        .appName("csv2postgres")\
        .config("spark.jars", "/opt/spark-apps/packages/postgresql-42.2.22.jar")\
        .getOrCreate()
    sc = sql.sparkContext
    logging.info("Spark session initialized successfully")
    return sql, sc

def main():
    """
    Main function for processing and storing csv files in postgreSQL database
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    # Connection configuration
    url = "jdbc:postgresql://localhost:5432/globant_challenge_db"
    properties = {
        "user": "globant_super_admin",
        "password": "pass1234",
        "driver": "org.postgresql.Driver"
    }
    # Path configuration
    root_csv_path = "../data/historic"
    tables = "*" # ["departments", "hired_employees", "jobs"]
    schema = 'dbo'
    file = "*.csv"
    
    # Initializing connections
    sql, _ = init_spark()
    engine = create_engine("postgresql://globant_super_admin:pass1234@localhost:5432/globant_challenge_db")
    metadata = MetaData(bind=engine)

    # Getting table names
    if tables == "*":
        tables = os.walk(root_csv_path).next()[1]

    logging.info(f"Tables to ingest {tables}")
    for table_name in tables:
        full_path = os.path.join(root_csv_path, tables, file)
        exists_pg_table = engine.dialect.has_table(engine, f"{schema}.{table_name}")
        
        # Perfom read
        logging.info(f"Reading data from {full_path}")
        df = sql.read.load(full_path, format="csv", inferSchema="true", sep="\t", header="true")
        df = df.drop("id")

        # TODO: Automatic mapping to datatypes
        if not exists_pg_table:
            # Create the table if it does not exist
            logging.info(f"Table {schema}.{table_name} does not exist. Creating table")
            Table(table_name, metadata,
                  Column("id", Integer, primary_key=True),
                *[Column(c, String(60)) for c in df.columns]
            ).create()

        # Perform write in append mode
        logging.info(f"Writing data to {schema}.{table_name}")
        df.write \
          .jdbc(url=url, table=f"{schema}.{table_name}", mode='append', properties=properties) \
          .save()

    logging.info(f"Finish")
  
if __name__ == '__main__':
    main()