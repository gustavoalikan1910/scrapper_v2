import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, col
import os
import shutil
import re
from concurrent.futures import ThreadPoolExecutor
from delta.tables import DeltaTable

# Verifica se os parâmetros foram passados
if len(sys.argv) != 3:
    print("Uso: python delta.py <competicao> <source>")
    sys.exit(1)

competicao_param = sys.argv[1]  # Obtém o nome da competição como argumento
source_param = sys.argv[2]  # Obtém o nome da fonte (fbref ou ogol)

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
    .getOrCreate()

# Configuração de caminhos
input_path = "/home/jovyan/json"
processed_path = "/home/jovyan/json/processed"
output_base_path = "/home/jovyan/delta_tables/bronze"

# Definir regex com base no parâmetro source
if source_param == "fbref":
    pattern = re.compile(r"(?P<competicao>\w+)_(?:(?P<nivel>\w+)_(?P<granularidade>\w+))?_stats_(?P<tipo>\w+)_(?P<ano>\d{4})\.json")
elif source_param == "ogol":
    pattern = re.compile(r"(?P<competicao>\w+)_total_(?P<tipo>\w+?)_(?P<nivel>\w+)_(?P<ano>\d{4})\.json")
else:
    print("Source inválido. Use 'fbref' ou 'ogol'.")
    sys.exit(1)

# Lista e categoriza os arquivos na pasta de entrada
arquivos = os.listdir(input_path)
dados_estruturados = []

for arquivo in arquivos:
    match = pattern.match(arquivo)
    if match:
        info = match.groupdict()
        info['arquivo'] = arquivo
        if info['competicao'] == competicao_param:  # Filtra pela competição passada
            dados_estruturados.append(info)

# Função para sanitizar os nomes das colunas
def sanitize_column_names(df):
    for col in df.columns:
        sanitized_name = col.replace(" ", "_").replace(".", "_").replace(";", "_") \
                             .replace("{", "").replace("}", "").replace("(", "").replace(")", "") \
                             .replace("\n", "").replace("\t", "").replace("=", "")
        if col != sanitized_name:
            print(f"Renomeando coluna: De '{col}' para '{sanitized_name}'")
        df = df.withColumnRenamed(col, sanitized_name)
    return df

# Função para processar um único arquivo
def processar_arquivo(dado):
    arquivo = dado['arquivo']
    competicao = dado['competicao']
    nivel = dado.get('nivel', 'unknown')
    granularidade = dado.get('granularidade') if source_param == "fbref" else None
    tipo = dado['tipo']
    ano = dado['ano']

    caminho_arquivo = f"{input_path}/{arquivo}"

    # Garantir que o diretório 'processed_path' existe
    if not os.path.exists(processed_path):
        os.makedirs(processed_path)
    
    try:
        print(f"Lendo o arquivo: {caminho_arquivo}")
        df = spark.read.json(caminho_arquivo)

        df = sanitize_column_names(df)

        df = df.withColumn("competicao", lit(competicao)) \
               .withColumn("nivel", lit(nivel)) \
               .withColumn("granularidade", lit(granularidade if granularidade else "")) \
               .withColumn("tipo", lit(tipo)) \
               .withColumn("ano", lit(ano)) \
               .withColumn("arquivo_origem", input_file_name())

        # Define o caminho de saída baseado na granularidade
        if granularidade:
            output_path = f"{output_base_path}/{competicao}_{nivel}_{granularidade}_{tipo}"
        else:
            output_path = f"{output_base_path}/{competicao}_{nivel}_{tipo}"

        print(f"Atualizando tabela Delta em: {output_path}")

        if DeltaTable.isDeltaTable(spark, output_path):
            # Carrega a tabela Delta existente
            delta_table = DeltaTable.forPath(spark, output_path)

            # Define o ano do `source` para exclusão no `target`
            ano_to_update = df.select("ano").distinct().collect()[0]["ano"]

            # Apaga todas as linhas do ano no `target`
            print(f"Apagando linhas do ano {ano_to_update} na tabela Delta...")
            delta_table.delete(col("ano") == ano_to_update)

            # Insere as novas linhas do `source`
            print(f"Inserindo novas linhas do ano {ano_to_update} na tabela Delta...")
            df.write.format("delta").mode("append").save(output_path)
        else:
            # Cria uma nova tabela Delta se não existir
            df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("ano") \
                .save(output_path)

        # Move o arquivo para a pasta processed
        destino_arquivo = f"{processed_path}/{arquivo}"
        shutil.move(caminho_arquivo, destino_arquivo)
        print(f"Arquivo movido para: {destino_arquivo}")

    except Exception as e:
        print(f"Erro ao processar {arquivo}: {e}")

with ThreadPoolExecutor(max_workers=1) as executor:
    executor.map(processar_arquivo, dados_estruturados)

print(f"Processamento concluído para a competição: {competicao_param} com source: {source_param}!")
