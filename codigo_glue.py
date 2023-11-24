import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Inicializar o contexto Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Obter as opções fornecidas ao script
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Carregar dados da tabela de origem para um DynamicFrame
source_table = "seu_banco_de_dados.sua_tabela_origem"
source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database="seu_banco_de_dados", table_name="sua_tabela_origem")

# Converter DynamicFrame para DataFrame para aplicar transformações
source_dataframe = source_dynamic_frame.toDF()

# Aplicar transformações, se necessário (opcional)
# Exemplo: Adicionar uma coluna com uma transformação simples
transformed_dataframe = source_dataframe.withColumn("nova_coluna_destino", source_dataframe["coluna_origem"])

# Especificar a tabela de destino e a coluna de partição
output_table = "seu_banco_de_dados.sua_tabela_destino"
partition_column = "sua_coluna_de_particao"

# Salvar o DataFrame na tabela de destino usando append e particionamento
transformed_dataframe.write.mode("append").partitionBy(partition_column).saveAsTable(output_table)
