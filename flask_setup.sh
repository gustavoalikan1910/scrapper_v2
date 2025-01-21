#!/bin/bash

echo "Iniciando o setup do Flask..."

# Instala o Flask se não estiver instalado
pip show flask > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Flask não encontrado. Instalando Flask..."
    pip install flask
else
    echo "Flask já está instalado."
fi

# Instalar outras bibliotecas necessárias
echo "Instalando bibliotecas adicionais..."
pip install flask-jwt-extended flask-limiter psycopg2-binary pytz

# Mensagem final
echo "Setup do Flask finalizado com sucesso!"
