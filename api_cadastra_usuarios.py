from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'api_cadastra_usuarios',
    default_args=default_args,
    description='Chama a API Flask para executar o script',
    schedule_interval='30 * * * *',  # Executa no minuto 30 de cada hora
    start_date=datetime(2023, 1, 1),
    catchup=False,
)


# Função para chamar a API
def chamar_api():
    try:
        
        url = "http://132.226.254.126:5001/executar-script"  # URL da API Flask
        headers = {
            "email": "airflow@gmail.com"  # Passa o email no cabeçalho
        }
        
        # Faz a requisição POST
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        
        # Log do resultado
        print("API executada com sucesso:", response.json())
    except Exception as e:
        print(f"Erro ao chamar a API Flask: {e}")
        raise

# Tarefa do Airflow
executar_script = PythonOperator(
    task_id='chamar_api_flask',
    python_callable=chamar_api,
    dag=dag,
)

executar_script
