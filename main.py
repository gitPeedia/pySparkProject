from src import CSVBuilder
from src import dFBuilder
import glob
from datetime import datetime

######  imports spark ######
import pyspark
from pyspark.sql import DataFrame, SparkSession, functions
spark = SparkSession.builder.appName("CSVBuilder").getOrCreate()

##################  update if your file will have a header  ##################
first_row_is_header = "true"
##################  set here the delimiter of your source file    ############
delimiter = "§"
##################  set here the type of your source file   ##################
file_type = "orc"
######  parameter can be used if you want to group your files by date   ######
now = datetime.now()
######  parameter can be used if you want to partition your files ############
num_rows_partition = 1000000
##################          make the paths parameters         ################
table_name = "contrato"
origin_path = "aux/orc/contrato/{}/".format(table_name)
target_path = "aux/csv/{}/".format(table_name)

##  com adiçao de campos ##
#struct = json.loads(open("aux/json/estrutura_outra.json", "r").read())
#dFBuilder.tempView(struct, origin_path, file_type, first_row_is_header, delimiter)
#result = spark.sql("select cont.* from contrato cont ")
#CSVBuilder.gravaCSV(result, 'csv', ',', target_path, target_path.format(table_name), True, num_rows_partition, table_name, first_row_is_header, None)

##  sem adiçao de campos ##
for file in glob.glob("{}*.{}".format(origin_path, file_type)):
    df = CSVBuilder.lerCSV(file, file_type, first_row_is_header, delimiter, None)
    CSVBuilder.gravaCSV(df, 'csv', ',', target_path, target_path.format(table_name), True, num_rows_partition, table_name, first_row_is_header, None)