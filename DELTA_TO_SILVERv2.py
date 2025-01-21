import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sys
import time

# Configuração da SparkSession para trabalhar com tabelas Delta
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
    .config("spark.executor.instances", "2") \
    .getOrCreate()


# Verifica se os parâmetros foram passados
if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Uso: python delta.py <competicao> [ano|current]")
    sys.exit(1)

competicao_param = sys.argv[1]  # Obtém o nome da competição como argumento
ano_param = sys.argv[2] if len(sys.argv) == 3 else "current"

# Determina o ano com base no parâmetro
ano_corrente = datetime.now().year if ano_param.lower() == "current" else ano_param
base_path = "/home/jovyan/delta_tables/bronze"  # Caminho base das tabelas Delta

# # Configurações de caminhos e parâmetros
#base_path = "/home/jovyan/delta_tables/bronze"  # Caminho base das tabelas Delta
# competicao_param = 'brasileirao'  # Nome da competição
# ano_param = '2024'
# ano_corrente = datetime.now().year if ano_param.lower() == "current" else ano_param



# Configurações do PostgreSQL
pg_url = "jdbc:postgresql://postgres_db:5432/meu_banco"
pg_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

def test_jdbc_connection(spark, pg_url, pg_properties):
    try:
        print("Testando conexão ao PostgreSQL...")
        test_query = "(SELECT 1) AS test"
        spark.read.jdbc(url=pg_url, table=test_query, properties=pg_properties)
        print("Conexão ao PostgreSQL testada com sucesso!")
    except Exception as e:
        print(f"Erro ao testar conexão ao PostgreSQL: {e}")
        sys.exit(1)  # Encerra o script se o teste falhar
        

def find_delta_partition_paths(base_path, competition_name, ano_corrente):
    """
    Localiza as tabelas Delta para a competição e ano especificados.
    """
    print(f"Procurando tabelas Delta em: {base_path}")
    paths = []
    for root, dirs, files in os.walk(base_path):
        for dir_name in dirs:
            if dir_name == f"ano={ano_corrente}" and competition_name in root:
                path = os.path.join(root, dir_name)
                #print(f"Partição encontrada: {path}")
                paths.append(path)
    return paths


def delete_records_with_spark(spark, pg_url, pg_properties, table_name, ano_corrente):
    """
    Executa DELETE no PostgreSQL para um ano específico usando Spark JDBC.
    """
    try:
        delete_query = f"DELETE FROM silver_estatisticas_futebol.{table_name} WHERE ano = '{ano_corrente}'"
        print(f"Executando DELETE na tabela {table_name} para o ano {ano_corrente}...")
        
        # Configuração para execução direta
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            pg_url, pg_properties['user'], pg_properties['password']
        )
        stmt = conn.createStatement()
        stmt.execute(delete_query)
        conn.close()
        
        print(f"Registros do ano {ano_corrente} deletados na tabela {table_name}.")
    except Exception as e:
        print(f"Erro ao deletar registros na tabela {table_name}: {e}")

def append_data_with_spark(spark, df, pg_url, pg_properties, table_name):
    """
    Insere os dados no PostgreSQL usando a API Spark JDBC.
    """
    try:
        print(f"Inserindo novos dados na tabela {table_name}...")
        df.write \
            .jdbc(url=pg_url, table=f"silver_estatisticas_futebol.{table_name}", mode="append", properties=pg_properties)
        print(f"Dados inseridos com sucesso na tabela {table_name}!")
        print("")
    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")
        print("")


def process_and_save_tables(spark, base_path, pg_url, pg_properties, competition_name, ano_corrente):
    """
    Lê todas as tabelas Delta que começam com o nome da competição,
    deleta dados antigos no PostgreSQL e insere os novos registros.
    """
    table_paths = find_delta_partition_paths(base_path, competition_name, ano_corrente)
    if not table_paths:
        print(f"Nenhuma tabela encontrada para {competition_name} no ano {ano_corrente}")
        return

    for table_path in table_paths:
        table_name = table_path.split("/")[-2]
        print(f"Processando tabela: {table_name}, Ano: {ano_corrente}")

        try:
            # Lê a partição do ano fornecido
            base_table_path = os.path.dirname(table_path)  # Diretório base da tabela
            #print(base_table_path)
            df = spark.read.format("delta").load(base_table_path).where(f"ano = {ano_corrente}")

            print(f"Tabela carregada: {table_name} com {df.count()} registros para o ano {ano_corrente}.")

            # Adiciona a coluna de data de inserção
            df_with_date = df.withColumn("data_insercao", F.current_timestamp())

            # Deleta registros antigos no PostgreSQL
            delete_records_with_spark(spark, pg_url, pg_properties, table_name, ano_corrente)

            # Insere os novos dados no PostgreSQL
            append_data_with_spark(spark, df_with_date, pg_url, pg_properties, table_name)

        except Exception as e:
            print(f"Erro ao processar a tabela {table_name}: {e}")


# Executa o processamento
# Carrega o driver JDBC explicitamente na JVM
test_jdbc_connection(spark, pg_url, pg_properties)
print("Driver PostgreSQL carregado com sucesso.")
print("")
print("Aguardando 10 segundos para estabilizar o ambiente...")
time.sleep(10)
print("")
process_and_save_tables(spark, base_path, pg_url, pg_properties, competicao_param, ano_corrente)
print("")
print("Script Finalizado")