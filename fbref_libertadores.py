from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

# Função que executa o script CAPTURA_DADOS_FREF_TO_LINUX.py
def run_fbref_script():
    script_path = "/home/jovyan/CAPTURA_DADOS_FREF_TO_LINUX.py"
    python_path = "/usr/local/bin/python"
    competicao = "libertadores"
    reproc = "False"
    environment = "prod"
    
    try:
        result = subprocess.run(
            [python_path, script_path, competicao, reproc, environment],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        log.info(f"Saída do script FBREF:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        log.error(f"Erro ao executar o script FBREF:\n{e.stderr}")
        raise

# Função que executa o script Delta.py
def run_delta_script():
    script_path = "/home/jovyan/BRONZE_DELTA_TABLE.py"
    python_path = "/usr/local/bin/python"
    competicao = "libertadores"
    source = "fbref"
    
    
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

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fbref_libertadores",
    default_args=default_args,
    description="Executa o script CAPTURA_DADOS_FREF_TO_LINUX.py, libertadores parametrizado e transforma em Delta",
    schedule_interval="0 16 * * *",  # Cron para 9AM UTC
    start_date=datetime(2024, 12, 22),
    catchup=False,
    tags=["fbref", "libertadores"],
) as dag:

    # Tarefa 1: Executa o script fbref_libertadores.py
    get_data_fbref = PythonOperator(
        task_id="Captura_Dados_Fbref_libertadores",
        python_callable=run_fbref_script,
    )

    # Tarefa 2: Transforma os dados em Delta para a competição libertadores
    transform_to_delta = PythonOperator(
        task_id="Salva_Delta_Table_Bronze",
        python_callable=run_delta_script,
    )

    # Define a ordem de execução
    get_data_fbref >> transform_to_delta
