import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# Verifica se os parâmetros foram passados
if len(sys.argv) != 2:
    print("Uso: python delta.py <competicao>")
    sys.exit(1)

competicao_param = sys.argv[1]  # Obtém o nome da competição como argumento

# Inicializa a SparkSession com as configurações otimizadas
spark = SparkSession.builder \
    .appName("ProcessarJSONParaDelta") \
    .master("local[4]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "8") \
    .config("spark.executor.memory", "20g") \
    .config("spark.driver.memory", "20g")  \
    .config("spark.memory.fraction", "0.95") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.shuffle.file.buffer", "64k") \
    .config("spark.network.timeout", "300s") \
    .config("spark.rpc.message.maxSize", "256") \
    .config("spark.broadcast.blockSize", "8m") \
    .config("spark.sql.files.maxPartitionBytes", "256m") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50m") \
    .config("spark.speculation", "true") \
    .config("spark.speculation.quantile", "0.9") \
    .config("spark.speculation.multiplier", "1.2") \
    .config("spark.rdd.compress", "true") \
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
    .config("spark.sql.debug.maxToStringFields", "1000")  \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
    .config("spark.jars", "/tmp/postgresql-42.5.4.jar") \
    .getOrCreate()

# Caminho base para as tabelas Delta
base_path = "/home/jovyan/delta_tables/bronze"

# Configurações do PostgreSQL
pg_url = "jdbc:postgresql://postgres_db:5432/meu_banco"
pg_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

def process_and_save_tables(competition_name):
    """
    Lê todas as tabelas Delta que começam com o nome da competição,
    adiciona uma coluna de data de inserção e salva no PostgreSQL.
    """
    # Usar glob para encontrar todos os caminhos que correspondem ao padrão
    table_paths = glob.glob(f"{base_path}/{competition_name}_*")
    if not table_paths:
        print(f"Nenhuma tabela encontrada para {competition_name}")
        return

    for table_path in table_paths:
        table_name = table_path.split("/")[-1]  # Nome da tabela baseado no nome do diretório
        print(f"Processando tabela: {table_name}")

        try:
            # Lê a tabela Delta
            df = spark.read.format("delta").load(table_path)
            print(f"Tabela carregada: {table_name}")

            # Adiciona a coluna de data de inserção
            df_with_date = df.withColumn("data_insercao", F.current_timestamp())

            # Salva a tabela no PostgreSQL
            print(f"Salvando tabela no PostgreSQL: {table_name}")
            df_with_date.write \
                .jdbc(url=pg_url, table=f"silver_estatisticas_futebol.{table_name}", mode="overwrite", properties=pg_properties)
            print(f"Tabela {table_name} salva com sucesso!")
        except Exception as e:
            print(f"Erro ao processar a tabela {table_name}: {e}")

# Nome do campeonato como parâmetro
#competition_name = "brasileirao"  # Por exemplo, 'brasileirao'
process_and_save_tables(competicao_param)
