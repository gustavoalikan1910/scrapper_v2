from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# Função que executa o script CAPTURA_DADOS_OGOL_TO_LINUX.py
def run_ogol_script():
    script_path = "/home/jovyan/CAPTURA_DADOS_OGOL_TO_LINUX.py"
    python_path = "/usr/local/bin/python"
    competicao = "193105_copinha-2025"
    # 180577_copinha-2024
    
    try:
        result = subprocess.run(
            [python_path, script_path, competicao],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        log.info(f"Saída do script OGOL:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        log.error(f"Erro ao executar o script OGOL:\n{e.stderr}")
        raise

# Função que executa o script Delta.py
def run_delta_script():
    script_path = "/home/jovyan/BRONZE_DELTA_TABLE.py"
    python_path = "/usr/local/bin/python"
    competicao = "copinha"
    source = "ogol"
    
    
    try:
        result = subprocess.run(
            [python_path, script_path, competicao, source],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        log.info(f"Saída do script Delta:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        log.error(f"Erro ao executar o script Delta:\n{e.stderr}")
        raise

# Função que executa o script DELTA_TO_SILVER.py
def run_silver_script():
    script_path = "/home/jovyan/DELTA_TO_SILVERv2.py"
    python_path = "/usr/local/bin/python"
    competicao = "copinha"
    ano = '2024'
    
    try:
        result = subprocess.run(
            [python_path, script_path, competicao, ano],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        log.info(f"Saída do script Delta:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        log.error(f"Erro ao executar o script Delta:\n{e.stderr}")
        raise

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ogol_copinha",
    default_args=default_args,
    description="Executa o script CAPTURA_DADOS_OGOL_TO_LINUX.py, copinha parametrizado e transforma em Delta",
    schedule_interval="0 0 * * *",  # Cron para 9AM UTC
    start_date=datetime(2024, 12, 22),
    catchup=False,
    tags=["ogol", "copinha"],
) as dag:

    # Tarefa 1: Executa o script ogol_copinha.py
    get_data_ogol = PythonOperator(
        task_id="Captura_Dados_Ogol_copinha",
        python_callable=run_ogol_script,
    )

    # Tarefa 2: Transforma os dados em Delta para a competição copinha
    transform_to_delta = PythonOperator(
        task_id="Salva_Delta_Table_Bronze_copinha",
        python_callable=run_delta_script,
    )

    # Tarefa 3: Le delta table e insere na silver
    load_silver_tables = PythonOperator(
        task_id="Carrega_silver_tables_copinha",
        python_callable=run_silver_script,
    )
    
    # Define a ordem de execução
    get_data_ogol >> transform_to_delta >> load_silver_tables
