#!/bin/bash

# Ativar sudo sem senha para o usuário jovyan
echo "jovyan ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Criar e configurar o ambiente virtual
if [ ! -d "/home/jovyan/venv_soccerdata" ]; then
    python3 -m venv /home/jovyan/venv_soccerdata
    source /home/jovyan/venv_soccerdata/bin/activate
    pip install --upgrade pip
    pip install soccerdata ipykernel
    python -m ipykernel install --user --name=venv_soccerdata --display-name "Python (venv_soccerdata)"
    deactivate
fi

# Criar diretório e arquivo de configuração para o soccerdata
SOCCERDATA_CONFIG_DIR="/home/jovyan/soccerdata/config"
LEAGUE_DICT_FILE="$SOCCERDATA_CONFIG_DIR/league_dict.json"

# Criar diretório se não existir
if [ ! -d "$SOCCERDATA_CONFIG_DIR" ]; then
    mkdir -p "$SOCCERDATA_CONFIG_DIR"
fi

# Criar arquivo league_dict.json se não existir
if [ ! -f "$LEAGUE_DICT_FILE" ]; then
    cat <<EOL > "$LEAGUE_DICT_FILE"
{
    "brasileirao": {
        "FBref": "Campeonato Brasileiro Série A",
        "season_start": "Jan",
        "season_end": "Dec"
    },
    "sulamericana": {
        "FBref": "Copa Sudamericana",
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
fi

# Iniciar o Jupyter Lab
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=''
