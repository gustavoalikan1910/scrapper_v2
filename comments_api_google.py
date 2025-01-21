from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
from airflow.utils.log.logging_mixin import LoggingMixin
import os
import sys

# Configuração do log do Airflow
log = LoggingMixin().log

# Função que executa o script GERADOR_COMMENTS_POSTGRES_API_GOOGLE.py
def run_script():
    script_path = "/home/jovyan/GERADOR_COMMENTS_POSTGRES_API_GOOGLE.py"
    python_path = "/usr/local/bin/python"

    # Garante que a saída seja desbufferizada
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    try:
        # Executa o script com saída desbufferizada
        process = subprocess.Popen(
            [python_path, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,  # Garante saída desbufferizada
        )

        # Captura o stdout em tempo real
        while True:
            output = process.stdout.readline()
            if output:
                log.info(output.strip())  # Exibe a saída imediatamente
            error = process.stderr.readline()
            if error:
                log.error(error.strip())  # Exibe erros imediatamente
            if output == "" and process.poll() is not None:
                break

        # Verifica o código de retorno do processo
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, [python_path, script_path])
    except subprocess.CalledProcessError as e:
        log.error(f"Erro ao executar o script: Código de retorno {e.returncode}")
        raise

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="comments_api_google",
    default_args=default_args,
    description="Executa o script GERADOR_COMMENTS_POSTGRES_API_GOOGLE.py",
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 12, 22),
    catchup=False,
    tags=["comments", "api_google"],
) as dag:

    # Tarefa 1: Executa o script GERADOR_COMMENTS_POSTGRES_API_GOOGLE.py
    run_comments = PythonOperator(
        task_id="get_comments_api_google",
        python_callable=run_script,  # Chama a função que executa o script
    )

    # Define a ordem de execução
    run_comments
