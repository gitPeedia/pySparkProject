from src import CSVBuilder
from src import dFBuilder
from getname import random_name
import glob
import json
from datetime import datetime

######  spark ######
import pyspark
from pyspark.sql import DataFrame, SparkSession, functions
spark = SparkSession.builder.appName("CSVBuilder").getOrCreate()

######  CSV criaçao ######
first_row_is_header = "true"
delimiter = "§"
file_type = "orc"
now = datetime.now()
table_name = "assistencia_servico_tb"
num_rows_partition = 1000000
origin_path = "aux/orc/{}/".format(table_name)
path = "aux/csv/{}/".format(table_name)
#origin_path = "aux/orc/{}-{}/".format(now.day, '{:02d}'.format(now.month))
#part-00000-29c5cc7a-d07a-4858-a590-b955747f4e9d.snappy.orc

##  com adiçao de capos ##
struct = json.loads(open("aux/json/estrutura_outra.json", "r").read())
dFBuilder.tempView(struct, origin_path, file_type, first_row_is_header, delimiter)
df = spark.sql("select aserv.*, cat.nome from assistencia_servico_tb aserv join assistencia_categoria_servico_tb cat on aserv.categoria_servico_id = cat.categoria_servico_id")
df = df.where(df['usuario'] == 'CARGA_BB_3.0')
result = df.select(df['linha'], df['nome'], df['ordem'], df['data_exportacao'], df['plano_assistencia_id'], df['servico_id'], df['categoria_servico_id'])
CSVBuilder.gravaCSV(result, 'csv', ',', path, path.format(table_name), True, num_rows_partition, table_name, first_row_is_header, None)

##  sem adiçao de capos ##
#print(origin_path)
#for file in glob.glob("{}*.{}".format(origin_path, file_type)):
#    df = CSVBuilder.lerCSV(file, file_type, first_row_is_header, delimiter, None)
#    df = df.where(df['usuario'] == 'CARGA_BB_3.0')
#    result = df.select(df['dt_alteracao'], df['dt_inclusao'], df['plano_assistencia_id'], df['servico_id'])
#    CSVBuilder.gravaCSV(result, 'csv', ',', path, path.format(table_name), True, num_rows_partition, table_name, first_row_is_header, None)