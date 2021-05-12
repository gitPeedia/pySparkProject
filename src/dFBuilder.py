import glob
import re
from src import CSVBuilder

######      SPARK       ######
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit
spark = SparkSession.builder.appName("CSVBuilder").getOrCreate()

######  JSON TEMP TABLES ######
def tempView(struct, path, file_type, first_row_is_header, delimiter):
    for db, tables in struct.items():
        print(path)
        for table, param in tables.items():
            for file in glob.glob("{}{}/*.{}".format(path, table, file_type)):
                print(file)
                CSVBuilder.lerCSV(file, file_type, first_row_is_header, delimiter, table)

            campos = param.get("estrutura")
            pks = param.get("pk")

            query = '\n select '

            if (len(campos) > 0):
                for item in campos:
                    query += str(item[0])+', '
            else:
                query += '*, '

            queryTempTable = '{} row_number() over (partition by {} order by data_exportacao desc) as linha from {}'.format(query, ', '.join(pks), table)
            print(table)
            dataframe = spark.sql(queryTempTable)
            dataframe.registerTempTable(table);