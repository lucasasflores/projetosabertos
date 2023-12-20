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


def list_objects(bucket_name, prefix='', continuation_token=None, object_uris=None):
    if object_uris is None:
        object_uris = []  # Inicializa a lista se não existir
    
    s3 = boto3.client('s3')
    
    # Parâmetros iniciais
    operation_parameters = {'Bucket': bucket_name, 'Prefix': prefix}
    
    # Adicionar o token de continuação se existir
    if continuation_token:
        operation_parameters['ContinuationToken'] = continuation_token
    
    # Chamada para list_objects_v2
    result = s3.list_objects_v2(**operation_parameters)
    
    # Processar objetos da página atual
    object_uris.extend([f"s3://{obj['Bucket']}/{obj['Key']}" for obj in result.get('Contents', [])])
    
    # Verificar se há mais páginas e chamar recursivamente se necessário
    if result.get('IsTruncated', False):
        next_continuation_token = result.get('NextContinuationToken')
        list_objects(bucket_name, prefix, next_continuation_token, object_uris)
    
    return object_uris