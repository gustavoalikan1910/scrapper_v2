import pandas as pd  # type: ignore
import time
import warnings
import gc
import os
from datetime import datetime
import sys
import subprocess
# from airflow.utils.log.logging_mixin import LoggingMixin

# Verifica se o parâmetro de competição foi passado
if len(sys.argv) != 2:
    print("Uso: python delta.py <competicao>.")
    sys.exit(1)

competicao_param = sys.argv[1]

# Configure o logger do Airflow
#log = LoggingMixin().log

# Suprimir o aviso específico do BeautifulSoup
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

competicao = [competicao_param]

# Pasta de saída para os arquivos JSON
#output_folder = r'C:\Users\galika21\soccerdata\output_files'
output_folder = '/home/jovyan/json'

def process_and_export_to_json(df, df_name, output_folder):
    # Criar a pasta de saída se não existir
    os.makedirs(output_folder, exist_ok=True)

    # Renomear as colunas
    def clean_column_name(col):
        if isinstance(col, tuple):
            col = '_'.join(map(str, col))  # Converte a tupla em string
        return col.replace(',', '').replace('/', '').replace(':', '_').replace(' ', '').replace("'", '').replace('%', '_percent')

    df.columns = [clean_column_name(col) for col in df.columns]

    # Resetar índices transformando tudo em coluna
    #df = df.reset_index()

    # Nome do arquivo de saída
    output_file = os.path.join(output_folder, f"{df_name}.json")

    # Exportar para JSON
    df.to_json(output_file, orient='records', force_ascii=False)
    print(f"DataFrame '{df_name}' exportado para '{output_file}'.")

    # Apagar o DataFrame
    #del df
    #gc.collect()

for id_camp in competicao:
    id, camp_ano = id_camp.split('_', 1)

    # Separa a variável camp
    camp, _, ano = camp_ano.rpartition('-')
    camp = camp.replace('-', '')
    print(camp + " " + ano)

    # Criar um dicionário para armazenar todas as URLs
    urls = {
        camp+"_total_gols_marcados_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt1&v1=e&v2=t&v3=1&pais=0&pos=0&id_equipa=0&ord=d',
        camp+"_total_gols_sofridos_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et1&v1=e&v2=t&v3=2&id_equipa=0&ord=d',
        camp+"_total_desempenho_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et2&v1=e&v2=t&v3=3&id_equipa=0&ord=d',
        camp+"_total_disciplina_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et3&v1=e&v2=t&v3=4&id_equipa=0&ord=d',
        camp+"_total_curiosidades_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et1&v1=e&v2=t&v3=9&id_equipa=0&ord=d',
        camp+"_total_gols_sofridos_minuto_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et14&v1=e&v2=t&v3=13&id_equipa=0&ord=d',
        camp+"_total_gols_minuto_equipe_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et11&v1=e&v2=t&v3=11&id_equipa=0&ord=d',
        camp+"_total_gols_marcados_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=et9&v1=j&v2=t&v3=9&id_equipa=0&ord=d',
        camp+"_total_disciplina_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt9&v1=j&v2=t&v3=4&pais=0&pos=0&id_equipa=0&ord=d',
        camp+"_total_utilizacao_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt4&v1=j&v2=t&v3=5&pais=0&pos=0&id_equipa=0&ord=d',
        camp+"_total_gols_sofridos_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt5&v1=j&v2=t&v3=2&pais=0&pos=0&id_equipa=0&ord=d',
        camp+"_total_gols_jogo_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt2&v1=j&v2=t&v3=12&pais=0&pos=0&id_equipa=0&ord=d',
        camp+"_total_gols_minuto_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt12&v1=j&v2=t&v3=11&pais=0&pos=0&id_equipa=0&ord=d',
        camp+"_total_assistencias_jogador_"+ano: f'https://www.ogol.com.br/edicao/{camp_ano}/{id}/estatisticas?sc=0&v=jt11&v1=j&v2=t&v3=14&pais=0&pos=0&id_equipa=0&ord=d'
    }

    # Iterar sobre cada URL e processar os dados
    i = 0
    for key, url in urls.items():
        try:
            if i > 0:
                time.sleep(5)

            df = pd.read_html(url, encoding='latin1')[1]  # Selecionar o segundo dataframe
            
            # Verificar se a coluna 'Equipe' ou 'Jogador' existe
            if 'Equipe' in df.columns:
                df = df.dropna(subset=['Equipe'])  # Remover linhas com NaN na coluna 'Equipe'
                df = df[df['Equipe'] != 'Equipe']  # Remover linhas onde 'Equipe' é igual a 'Equipe'
                df = df.drop(columns=['Unnamed: 0'])  # Remover a coluna 'Unnamed: 0'
            elif 'Jogador' in df.columns:
                df = df.dropna(subset=['Jogador'])  # Remover linhas com NaN na coluna 'Jogador'
                df = df[df['Jogador'] != 'Jogador']  # Remover linhas onde 'Jogador' é igual a 'Jogador'
                df = df.drop(columns=['Unnamed: 0'])  # Remover a coluna 'Unnamed: 0'

                # Criar a coluna 'Equipe' extraindo o conteúdo entre colchetes
                df['Equipe'] = df['Jogador'].str.extract(r'\[(.*?)\]')
                
                # Remover o conteúdo entre colchetes da coluna 'Jogador'
                df['Jogador'] = df['Jogador'].str.replace(r'\s*\[.*?\]', '', regex=True)
            else:
                print(f"A coluna 'Equipe' ou 'Jogador' não existe em {key}")

            # Exportar o DataFrame processado para JSON
            process_and_export_to_json(df, key, output_folder)

        except Exception as e:
            print(f"Erro ao processar {key}: {e}")
        i += 1