# Databricks notebook source
# MAGIC %md #Import Libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark

# Access to blob storage for writing data
# Set spark config for storage account: MY_STORAGE_ACCOUNT_NAME
spark.conf.set('fs.azure.account.key.MY_STORAGE_ACCOUNT_NAME.blob.core.windows.net,
               'STORAGE_ACCOUNTKEY')

df_write_path = 'wasbs://MY_BLOB_NAME@MY_STORAGE_ACCOUNT_NAME.blob.core.windows.net/workbooks.csv'

# Access cosmos
endpoint = 'https://MY_COSMOS_DB_ACCOUNT_NAME.documents.azure.com:443/'
masterkey = 'COSMOS_ACCOUNTKEY'

# Base Configuration
dfConfig = {
  "Endpoint" : endpoint,
  "Masterkey" : masterkey,
  "preferredRegions" : "Central US;East US2",
  "Database" : "hospital-data",
  "Collection" : "workbooks", 
  #"SamplingRatio" : "1.0",
  #"schema_samplesize" : "1000",
  #"query_pagesize" : "2147483647",
  "query_custom" : """
  SELECT
    c.id, 
    c.hospital, 
    c.code,
    c.published 
  FROM c WHERE c.published = 'True'
  """
}

# Connect via Spark connector to create Spark DataFrame
df = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**dfConfig).load()
print(df.count())


(df
     .coalesce(1) # uncomment this line to ensure that the data is written to just one file
     .write
     .format("csv")
     .option("header", "true")
     .option("escape", "\"")
     .mode("overwrite") # overwrite any pre-existing data at the path that we're writing to
     .save(df_write_path)
)

# COMMAND ----------
