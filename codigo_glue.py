from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

# Inicializa o contexto Spark e o contexto Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Leitura dos dados da tabela1
tabela1 = glueContext.create_dynamic_frame.from_catalog(
    database="seu_banco_de_dados",
    table_name="tabela1"
)

# Transformação de colunas
tabela2 = tabela1.apply_mapping([
    ("nome", "string", "name", "string"),
    ("sobrenome", "string", "lastname", "string"),
    ("anomesdia", "string", "anomesdia", "string")
    # Adicione mais mapeamentos conforme necessário
])

# Escrita dos dados na tabela2 no formato Avro
glueContext.write_dynamic_frame.from_catalog(
    frame=tabela2,
    database="seu_banco_de_dados",
    table_name="tabela2",
    format="avro"
)