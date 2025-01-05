#!/bin/bash
set -e  # Para o script em caso de erro

echo "Iniciando o setup do Airflow Scheduler..."

# Atualizar o pip
echo "Atualizando o pip para a versão mais recente..."
pip install --upgrade pip


echo "Instalando pacotes necessários (soccerdata, pyspark)..."
pip install soccerdata
pip install pyspark==3.3.0
pip install delta-spark==2.2.0
pip install google-generativeai --break-system-packages
pip install pyttsx3 --break-system-packages
pip install SpeechRecognition --break-system-packages


# Testar se o pacote soccerdata está funcionando
echo "Testando o pacote soccerdata..."
python -c "import soccerdata; print('Soccerdata funcionando no Scheduler!')"

# Criar diretório e arquivo de configuração para o soccerdata
SOCCERDATA_CONFIG_DIR="/home/airflow/soccerdata/config"
LEAGUE_DICT_FILE="$SOCCERDATA_CONFIG_DIR/league_dict.json"

echo "Criando o diretório de configuração em $SOCCERDATA_CONFIG_DIR..."
mkdir -p "$SOCCERDATA_CONFIG_DIR"

if [ ! -f "$LEAGUE_DICT_FILE" ]; then
    echo "Criando o arquivo league_dict.json..."
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
    echo "Arquivo league_dict.json já existe em $LEAGUE_DICT_FILE."
fi

echo "Setup do Scheduler concluído!"

# Adicione esta linha ao final
touch /tmp/airflow_scheduler_ready
echo "Scheduler do Airflow inicializado com sucesso!"
