#!/bin/bash
set -e  # Para o script em caso de erro

# Aguardar 2 minutos antes de iniciar o setup
echo "Aguardando 10 segs antes de iniciar o setup..."
sleep 10  # Aguarda 120 segundos (2 minutos)



echo "Atualizando o pip para a versão mais recente..."
pip install --upgrade pip


echo "Instalando pacotes necessários (soccerdata, pyspark)..."
pip install soccerdata
pip install pyspark==3.3.0



# Testar se o pacote soccerdata está funcionando
echo "Testando o pacote soccerdata..."
python -c "import soccerdata; print('Soccerdata funcionando!')"

# Criar diretório e arquivo de configuração para o soccerdata
SOCCERDATA_CONFIG_DIR="/home/airflow/soccerdata/config"
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

echo "Setup do Airflow concluído!"

# Adicione esta linha ao final
touch /tmp/airflow_ready
echo "Airflow inicializado com sucesso!"

# Iniciar o Airflow webserver como processo principal
#exec airflow webserver -p 8080