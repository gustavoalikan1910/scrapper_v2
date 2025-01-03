import json
import os
import pandas as pd
import time
import gc
import sys 
from datetime import datetime
import subprocess
from airflow.utils.log.logging_mixin import LoggingMixin


# Verifica se o parâmetro de competição foi passado
if len(sys.argv) != 4:
    print("Uso: python delta.py <competicao>. Reprocessamento = <reproc>. Ambiente = <environment>")
    sys.exit(1)

competicao = sys.argv[1]
reproc = sys.argv[2]
environment = sys.argv[3]


# Configure o logger do Airflow
log = LoggingMixin().log


# Criação da função de ativação de proxy TOR
def start_tor():
    try:
        # Inicia o serviço Tor
        tor_process = subprocess.Popen(
            ['tor'], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True  # Garante que a saída será retornada como string, não bytes
        )
        print("Tor iniciado com sucesso! Aguardando resposta...")

        # Aguarda alguns segundos para permitir que o Tor inicie completamente
        time.sleep(10)

        # Captura a saída do Tor (stdout e stderr)
        stdout, stderr = tor_process.communicate(timeout=30)

        # Imprime a saída
        print("Saída do Tor:")
        print(stdout)
        print("Erros do Tor:")
        print(stderr)

        # Retorna o processo do Tor para controle posterior
        return tor_process
    except subprocess.TimeoutExpired:
        print("Tor está demorando muito para responder...")
        return tor_process
    except Exception as e:
        print(f"Erro ao iniciar o Tor: {e}")
        return None

# Start no proxy TOR
tor_process = start_tor()

# Define quais temporadas carregar
if reproc=="True":
    season_list = ['2024']
else:
    current_year = datetime.now().year
    season_list = [str(current_year)]

print(season_list)

# Lista de estatísticas
run_simple_stats = True
run_team_stats = True
run_player_stats = True
team_stats = ['team_season_stats','team_match_stats']
player_stats = ['player_season_stats','player_match_stats']
#player_stats = ['player_match_stats']
#simple_stats = ['leagues','seasons','schedule','lineup','events','shot_events']
simple_stats = ['leagues','seasons','schedule']
#simple_stats = ['leagues']
 

# Lista de tipos de estatísticas por environment

if environment == 'prod':
    ###  PRODUCTION
    team_season_stats = ['standard','keeper','keeper_adv','shooting','passing','passing_types','goal_shot_creation','defense','possession','playing_time','misc']
    team_match_stats = ['schedule','keeper','shooting','passing','passing_types','goal_shot_creation','defense','possession','misc']
    player_season_stats = ['standard','shooting','passing','passing_types','goal_shot_creation','defense','possession','playing_time','misc','keeper','keeper_adv']
    player_match_stats = ['summary','keepers','passing','passing_types','defense','possession']
    #player_match_stats = ['summary','keepers','passing','passing_types','defense','possession','misc']

elif environment == 'dev':
    ### DEV
    team_season_stats = ['passing']
    team_match_stats = ['shooting']
    player_season_stats = ['standard']
    player_match_stats = ['keepers']

else:
    print('Variavel environment não definida')


# Pasta de saída para os arquivos JSON
output_folder = '/home/jovyan/json'

def process_and_export_to_json(dataframes, output_folder):
    for df_name in dataframes:

        # Criar a pasta de saída se não existir
        os.makedirs(output_folder, exist_ok=True)

        # Obter o DataFrame pelo nome
        df = globals()[df_name]

        # Renomear as colunas
        def clean_column_name(col):
            if isinstance(col, tuple):
                col = '_'.join(map(str, col))  # Converte a tupla em string
            
            col = col.replace(',', '').replace('/', '').replace(':', '_').replace(' ', '').replace("'", '').replace('%', '_percent')
            
            # Remover underscore no final do nome da coluna, se existir
            if col.endswith('_'):
                col = col[:-1]
            
            return col
        
        df.columns = [clean_column_name(col) for col in df.columns]


        # Resetar índices transformando tudo em coluna
        df = df.reset_index()

        # Nome do arquivo de saída
        output_file = os.path.join(output_folder, f"{df_name}.json")

        # Exportar para JSON
        df.to_json(output_file, orient='records', force_ascii=False)
        print(f"DataFrame '{df_name}' exportado para '{output_file}'.")
        
        # Apagar o DataFrame
        del globals()[df_name]
        gc.collect()

import soccerdata as sd

# Obter o horário atual
hora_atual = datetime.now().hour

# Inicializar a variável copas com base no horário
copas = [competicao]

log.info("Iniciando o script")

l=0
for league in copas:

    if l>0:
      time.sleep(60)  # Aguarda 60 segundos entre cada leitura
    
    s=0
    for season in season_list:
        if s>0:
          time.sleep(2)  # Aguarda 60 segundos entre cada leitura
        
        print(f"Iniciando scrapping: {league} {season}")
        fbref = sd.FBref(leagues=league, seasons=int(season), proxy='tor')
        #fbref = sd.FBref(leagues=league, seasons=int(season))

        #
        # ITERAÇÕES SOB ESTATÍSTICAS EM SIMPLE
        #
        if run_simple_stats:
          i=0
          for main_stat in simple_stats:
            if i>0:
              time.sleep(2)  # Aguarda 60 segundos entre cada leitura

            dataframe_name = f'{league}_{main_stat}_{season}'

            if main_stat == 'seasons' or main_stat == 'leagues':
              exec(f"{dataframe_name} = fbref.read_{main_stat}(split_up_big5=False)")
              print(f"{dataframe_name} carregado com sucesso.")
              list_dataframes = [dataframe_name]
              # Chamar a função para processar e exportar os DataFrames
              process_and_export_to_json(list_dataframes, output_folder)
              
            elif main_stat == 'schedule':
              exec(f"{dataframe_name} = fbref.read_{main_stat}(force_cache=False)")
              print(f"{dataframe_name} carregado com sucesso.")
              list_dataframes = [dataframe_name]
              # Chamar a função para processar e exportar os DataFrames
              process_and_export_to_json(list_dataframes, output_folder)

            elif main_stat=='lineup' or main_stat=='events' or main_stat=='shot_events':
              print('lineup, events, shot_events')
              #exec(f"{dataframe_name} = fbref.read_{main_stat}(match_id=None, force_cache=False)")
              print(f"{dataframe_name} carregado com sucesso.")
              list_dataframes = [dataframe_name]
              # Chamar a função para processar e exportar os DataFrames
              process_and_export_to_json(list_dataframes, output_folder)

            else:
              print('simple_stats desconhecido')
            i=i+1

        else: print('run_simple_stats is False')

        #
        # ITERAÇÕES SOB ESTATÍSTICAS EM TEAMS
        #
        if run_team_stats:
            
          # Caminho para o arquivo JSON
          team_file_path = os.path.expanduser(f'{output_folder}/{league}_schedule_{season}.json')
          # Lendo o arquivo JSON
          with open(team_file_path, 'r') as file:
            data = json.load(file)
          
          # Convertendo os dados para um DataFrame
          teams_season = pd.DataFrame(data)
          
          # Obtendo a lista distinta de times da casa
          unique_home_teams = teams_season['home_team'].unique()

          i=0
          
          for main_stat in team_stats:
            if i>0:
              time.sleep(2)  # Aguarda 60 segundos entre cada leitura
            stats_list = globals()[main_stat]
            j=0
            # Loop para ler os dados e criar variáveis dinamicamente
            for stat in stats_list:
              if j>0:
                time.sleep(2)  # Aguarda 60 segundos entre cada leitura

              dataframe_name = f'{league}_{main_stat}_{stat}_{season}'

              if main_stat == 'team_season_stats':
                exec(f"{dataframe_name} = fbref.read_{main_stat}(stat_type='{stat}')")
                print(f"{dataframe_name} carregado com sucesso.")
                list_dataframes = [dataframe_name]
                # Chamar a função para processar e exportar os DataFrames
                process_and_export_to_json(list_dataframes, output_folder)

              elif main_stat == 'team_match_stats':
                t=0
                exec(f"{dataframe_name} = pd.DataFrame()")  # Inicializa o DataFrame vazio
                #filtered_teams = [team for team in unique_home_teams if team == 'Corinthians' or team =='Fortaleza']
                for teams in unique_home_teams:
                #for teams in filtered_teams:
                    if t>0:
                        time.sleep(2)  # Aguarda 60 segundos entre cada leitura
                    try:
                      exec(f"{dataframe_name}_2 = fbref.read_{main_stat}(stat_type='{stat}', opponent_stats=False, team='{teams}', force_cache=False)")
                      #exec(f"{dataframe_name} = pd.concat([{dataframe_name}, {dataframe_name}_2], ignore_index=True)")
                      #if eval(f"not {dataframe_name}_2.empty"):  # Verifica se o DataFrame não está vazio
                      exec(f"{dataframe_name} = pd.concat([{dataframe_name}, {dataframe_name}_2])")
                    except Exception as e:
                      print(f"Erro ao processar o time {teams}: {e}")
                      continue  # Continua o loop para o próximo time
                
                    t=t+1  
                print(f"{dataframe_name} carregado com sucesso.")
                list_dataframes = [dataframe_name]
                
                # Chamar a função para processar e exportar os DataFrames
                process_and_export_to_json(list_dataframes, output_folder)

              else:
                print('team_stats desconhecido')
              j=j+1
            i=i+1

        else: print('run_season_stats is False')

        ### PREPARA GAME_IDS PARA LEITURA DE EVENTOS 

        # Caminho para o arquivo JSON
        #file_path = r'C:\Users\galika21\soccerdata\output_files\schedule.json'
        file_path = os.path.expanduser(f'{output_folder}/{league}_schedule_{season}.json')
    
        # Ler o arquivo JSON
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Extrair os game_id onde o home_team ou away_team é "Corinthians"
        corinthians_game_ids = [
            game['game_id'] for game in data
            #if (game['home_team'] == 'Corinthians' or game['away_team'] == 'Corinthians') and game['game_id'] is not None
            if game['game_id'] is not None
        ]

        #
        # ITERAÇÕES SOB ESTATÍSTICAS EM PLAYERS
        #
        if run_player_stats:
          i=0
          for main_stat in player_stats:
            if i>0:
              time.sleep(2)  # Aguarda 60 segundos entre cada leitura
            stats_list = globals()[main_stat]
            j=0
            # Loop para ler os dados e criar variáveis dinamicamente
            for stat in stats_list:
              if j>0:
                time.sleep(2)  # Aguarda 60 segundos entre cada leitura 

              dataframe_name = f'{league}_{main_stat}_{stat}_{season}'

              if main_stat == 'player_season_stats':
                exec(f"{dataframe_name} = fbref.read_{main_stat}(stat_type='{stat}')")
                print(f"{dataframe_name} carregado com sucesso.")
                list_dataframes = [dataframe_name]
                # Chamar a função para processar e exportar os DataFrames
                process_and_export_to_json(list_dataframes, output_folder)

              elif main_stat == 'player_match_stats':
                k=0
                exec(f"{dataframe_name} = pd.DataFrame()")  # Inicializa o DataFrame vazio
                for game in corinthians_game_ids:
                  if k>0:
                    time.sleep(1)
                    
                  try:
                    exec(f"{dataframe_name}_2 = fbref.read_{main_stat}(stat_type='{stat}', match_id='{game}', force_cache=False)")
                    if eval(f"not {dataframe_name}_2.empty"):  # Verifica se o DataFrame não está vazio
                          exec(f"{dataframe_name} = pd.concat([{dataframe_name}, {dataframe_name}_2])")
                          
                  except Exception as e:
                      print(f"Erro ao processar o match {game}: {e}")
                      continue  # Continua o loop para o próximo time
                  k=k+1
                   
                print(f"{dataframe_name} carregado com sucesso.")
                list_dataframes = [dataframe_name]
                # Chamar a função para processar e exportar os DataFrames
                process_and_export_to_json(list_dataframes, output_folder)
              

              else:
                print('team_stats desconhecido')
              j=j+1
            i=i+1

        else: print('run_season_stats is False')
        s=s+1        
    l=l+1
print('Scrapper finalizado com sucesso')

if tor_process:
    tor_process.terminate()
    print("Tor encerrado com sucesso.")
    log.info("Tor encerrado com sucesso.")
