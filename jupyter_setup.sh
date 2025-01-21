#!/bin/bash

# Ativar sudo sem senha para o usuário jovyan
echo "jovyan ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Instalar o pacote soccerdata
pip install --upgrade pip
pip install soccerdata

echo "Instalando pacotes necessários (soccerdata, pyspark)..."
pip install soccerdata pyspark
pip install psycopg2-binary
pip install google.generativeai
pip install flask

echo "Instalando o pacote tor..."
# Instala o Tor sem pedir confirmação (usando o -y para aceitar automaticamente)
sudo apt-get update -y
sudo apt-get install -y tor
sudo apt-get install curl -y


# Testar se o pacote soccerdata está funcionando
echo "Testando o pacote soccerdata..."
python -c "import soccerdata; print('Soccerdata funcionando!')"

# Criar diretório e arquivo de configuração para o soccerdata
SOCCERDATA_CONFIG_DIR="/home/jovyan/soccerdata/config"
LEAGUE_DICT_FILE="$SOCCERDATA_CONFIG_DIR/league_dict.json"

if [ ! -d "$SOCCERDATA_CONFIG_DIR" ]; then
    mkdir -p "$SOCCERDATA_CONFIG_DIR"
    echo "Diretório de configuração criado em $SOCCERDATA_CONFIG_DIR."
fi

if [ ! -f "$LEAGUE_DICT_FILE" ]; then
    cat <<EOL > "$LEAGUE_DICT_FILE"
{
    "brasileirao": {
        "FBref": "Campeonato Brasileiro Série A",
        "season_start": "Jan",
        "season_end": "Dec"
    },
    "sulamericana": {
        "FBref": "Copa CONMEBOL Sudamericana",
        "season_start": "Jan",
        "season_end": "Dec"
    },
    "libertadores": {
        "FBref": "Copa Libertadores de América",
        "season_start": "Jan",
        "season_end": "Dec"
    }
}
EOL
    echo "Arquivo league_dict.json criado com sucesso em $LEAGUE_DICT_FILE!"
else
    echo "Arquivo league_dict.json já existe."
fi

# Ajustar permissões em toda a pasta /home/jovyan
JOVYAN_HOME="/home/jovyan"
if [ -d "$JOVYAN_HOME" ]; then
    echo "Ajustando permissões na pasta $JOVYAN_HOME..."
    chmod -R 777 "$JOVYAN_HOME"
    echo "Permissões ajustadas com sucesso!"
else
    echo "Pasta $JOVYAN_HOME não encontrada. Criando agora..."
    mkdir -p "$JOVYAN_HOME"
    chmod -R 777 "$JOVYAN_HOME"
    echo "Pasta $JOVYAN_HOME criada e permissões ajustadas!"
fi

# Criar as pastas /home/jovyan/json e /home/jovyan/delta_tables
JSON_DIR="$JOVYAN_HOME/json"
DELTA_DIR="$JOVYAN_HOME/delta_tables"

if [ ! -d "$JSON_DIR" ]; then
    mkdir -p "$JSON_DIR"
    echo "Pasta $JSON_DIR criada com sucesso!"
fi
q
if [ ! -d "$DELTA_DIR" ]; then
    mkdir -p "$DELTA_DIR"
    echo "Pasta $DELTA_DIR criada com sucesso!"
fi

# Ajustar permissões nas pastas criadas
chmod -R 777 "$JSON_DIR"
chmod -R 777 "$DELTA_DIR"
echo "Permissões ajustadas para $JSON_DIR e $DELTA_DIR!"

# Baixar o driver JDBC do PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.5.4.jar -P /tmp


# Adicione esta linha ao final
touch /tmp/jupyter_ready
echo "Jupyter inicializado com sucesso!"


# Iniciar o Jupyter Lab
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=''

