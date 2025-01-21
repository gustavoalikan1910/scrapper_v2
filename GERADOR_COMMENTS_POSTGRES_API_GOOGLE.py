import psycopg2
from pyspark.sql import SparkSession
import google.generativeai as genai
import logging
import time

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do PostgreSQL
pg_url = "jdbc:postgresql://postgres_db:5432/meu_banco"
pg_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}
pg_connection_params = {
    "dbname": "meu_banco",
    "user": "admin",
    "password": "admin",
    "host": "postgres_db",
    "port": 5432
}

# Configuração da API generativa
genai.configure(api_key="AIzaSyB_7iRpDv7bXqDETSgSFtJtJm2kul-c_FI")
MODEL = genai.GenerativeModel('gemini-1.5-flash')

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("ProcessarJSONParaDelta") \
    .master("local[4]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
    .config("spark.jars", "/tmp/postgresql-42.5.4.jar") \
    .getOrCreate()


def get_columns_without_comments(spark, pg_url, pg_properties, limit):
    """
    Lê as colunas sem comentários no PostgreSQL usando Spark.
    Limita o número de colunas retornadas a 'limit'.
    """
    query = f"""
        (
            SELECT 
                table_name, 
                column_name 
            FROM information_schema.columns 
            LEFT JOIN pg_catalog.pg_description ON (
                pg_description.objoid = (
                    SELECT c.oid 
                    FROM pg_catalog.pg_class c
                    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = columns.table_name
                    AND n.nspname = 'silver_estatisticas_futebol'
                ) AND pg_description.objsubid = columns.ordinal_position
            )
            WHERE table_schema = 'silver_estatisticas_futebol'
            AND table_name LIKE '%brasileirao%'
            AND description IS NULL
            LIMIT {limit}
        ) AS colunas_sem_comentarios
    """
    logger.info(f"Lendo até {limit} colunas sem comentários do PostgreSQL...")
    df_columns = spark.read.jdbc(url=pg_url, table=query, properties=pg_properties)
    return df_columns


def generate_comment(table_name, column_name, model):
    """
    Gera um comentário para a coluna usando a API generativa.
    """
    prompt = (
        f"Gera pra mim comentários (Mais direta) em portugues, "
        f"sobre essa coluna. Tabela:{table_name} Coluna:{column_name}"
        f"Essas colunas são referentes a dados de estatisticas de futebol, retirados do site FBREF"
        f"Preciso que seja gerado só o comentário e não o comando inteiro do comment. O comentário precisa ser resumido, direto e curto. Sem aspas simples ou duplas. "
        f"Usar no máximo 100 caracteres no comentario"
    )
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        logger.error(f"Erro ao gerar comentário para Tabela: {table_name}, Coluna: {column_name}. Erro: {e}")
        return None


def execute_comment_in_postgres(comment, connection_params):
    """
    Executa um único comentário no PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**connection_params)
        with conn:
            with conn.cursor() as cur:
                cur.execute(comment)
                logger.info(f"Executado: {comment}")
        conn.close()
    except Exception as e:
        logger.error(f"Erro ao executar comentário: {comment}. Erro: {e}")


def process_columns(df_columns, model, connection_params):
    """
    Processa as colunas para gerar e executar os comentários.
    """
    logger.info(f"Processando {df_columns.count()} colunas...")
    for row in df_columns.collect():
        table_name = row["table_name"]
        column_name = row["column_name"]
        #print(column_name)

        # Certifique-se de que o nome da coluna seja envolto por aspas duplas
        column_name_escaped = column_name.replace('"', '""')
        comment_text = generate_comment(table_name, column_name_escaped, model)

        if comment_text:
            comment = f'COMMENT ON COLUMN silver_estatisticas_futebol.{table_name}."{column_name_escaped}" IS \'{comment_text}\';'
            execute_comment_in_postgres(comment, connection_params)
        time.sleep(5)  # Pausa para evitar sobrecarga na API


def main():
    """
    Função principal para executar o fluxo completo.
    """
    try:
        logger.info("Iniciando o processo de geração de comentários...")
        
        # Define o limite de colunas a serem processadas
        limit = 100
        
        df_columns = get_columns_without_comments(spark, pg_url, pg_properties, limit=limit)
        
        if df_columns.count() == 0:
            logger.info("Nenhuma coluna sem comentários encontrada.")
            return

        process_columns(df_columns, MODEL, pg_connection_params)
        logger.info("Processo concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante o processo principal: {e}")


main()
