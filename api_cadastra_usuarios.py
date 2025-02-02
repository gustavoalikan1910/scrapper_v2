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

# Função para obter o token JWT
def obter_token_jwt():
    try:
        url = "http://132.226.254.126:5001/login"  # Substitua pelo endereço correto da API
        payload = {
            "email": "airflow@gmail.com"  # Substitua pelo email válido
        }
        
        # Faz o login e obtém o token
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        # Retorna o token
        token = response.json().get("access_token")
        if not token:
            raise Exception("Não foi possível obter o token JWT.")
        return token
    except Exception as e:
        print(f"Erro ao obter o token JWT: {e}")
        raise

# Função para chamar a API
def chamar_api():
    try:
        # Obtém o token JWT
        token = obter_token_jwt()
        
        url = "http://132.226.254.126:5001/executar-script"  # URL da API Flask
        headers = {
            "Authorization": f"Bearer {token}"
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
