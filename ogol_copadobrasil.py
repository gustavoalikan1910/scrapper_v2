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
    competicao = "183035_copa-do-brasil-2024"
    
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
    competicao = "copadobrasil"
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

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ogol_copadobrasil",
    default_args=default_args,
    description="Executa o script OGOL_COPADOBRASIL.py, copadobrasil parametrizado e transforma em Delta",
    schedule_interval="0 0 * * *",  # Cron para 9AM UTC
    start_date=datetime(2024, 12, 22),
    catchup=False,
    tags=["ogol", "copadobrasil"],
) as dag:

    # Tarefa 1: Executa o script ogol_copadobrasil.py
    get_data_ogol = PythonOperator(
        task_id="Captura_Dados_Ogol_copadobrasil",
        python_callable=run_ogol_script,
    )

    # Tarefa 2: Transforma os dados em Delta para a competição copadobrasil
    transform_to_delta = PythonOperator(
        task_id="Salva_Delta_Table_Bronze",
        python_callable=run_delta_script,
    )

    # Define a ordem de execução
    get_data_ogol >> transform_to_delta
