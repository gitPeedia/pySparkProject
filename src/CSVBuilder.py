## Bibliotecas
import os
import re

######  spark ######
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date, lit

spark = SparkSession.builder.appName("CSVBuilder").getOrCreate()

date = current_date().cast("string")

nPartitions = lambda x, y: 1 if x <= y else x//y

def lerCSV (file, file_type, first_row_is_header, delimiter, table):

    if (re.search("csv", file_type)):
        df = spark.read.format("csv") \
                .option("header", first_row_is_header) \
                .option("delimiter", delimiter) \
                .option('compression', 'gzip') \
                .load(file)
    else:
        df = spark.read.format(file_type) \
                .load(file)

    if(table is None):
        return df
    else:
        df.withColumn("data_exportacao", current_date().cast("string")) \
            .registerTempTable(table)
        return None

def gravaCSV(dataFrame, file_type, delimiter, path, part_path, compression, num_rows_partition, table, first_row_is_header, op):

    ####  IDENTIFICA SE O CSV E PARA CARGA INCREMENTAL ####
    if (table[0] == "#" and op != "cheia"):
        dataFrame = dataFrame \
        .withColumn("Op", lit(str(op))) \
        .withColumn("data_exportacao", date)

    dataFrame.write.format(file_type) \
        .option("header", first_row_is_header) \
        .option("delimiter", delimiter) \
        .mode("overwrite") \
        .save(path)

    if(compression):
        dataFrame = dataFrame.repartition(nPartitions(dataFrame.count(), num_rows_partition))

        dataFrame.write.format(file_type) \
            .option("header", first_row_is_header) \
            .option("delimiter", delimiter) \
            .option("scape", "\"") \
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .option('compression', 'gzip') \
            .save(part_path)