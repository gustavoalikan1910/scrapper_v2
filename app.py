from flask import Flask, request, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flasgger import Swagger
import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import uuid
from datetime import datetime
from pytz import timezone

app = Flask(__name__)

# Configuração JWT
app.config["JWT_SECRET_KEY"] = "f792048144e5b4595f10ae9fe02729b4082cf7fa97883be5ae3216a736098d44"
jwt = JWTManager(app)

# Configuração Flask-Limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["5 per minute"])
limiter.init_app(app)

# Configuração PostgreSQL
DB_CONFIG = {
    'dbname': 'meu_banco',
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres_db',
    'port': 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# Configuração do Flasgger
swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": 'apispec',
            "route": '/docs/apispec.json',
            "rule_filter": lambda rule: True,
            "model_filter": lambda tag: True,
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/"
}
swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "API de Estatísticas de Futebol",
        "description": "Documentação da API para gerenciar estatísticas de futebol.",
        "version": "1.0.0"
    },
    "host": "132.226.254.126:5001",
    "basePath": "/",
    "schemes": ["http", "https"],
    "tags": [
        {"name": "1. Autenticação", "description": "Endpoints relacionados à autenticação"},
        {"name": "2. Tabelas", "description": "Endpoints para listar tabelas disponíveis"},
        {"name": "3. Dados", "description": "Endpoints para manipular dados das tabelas"}
    ]
}
Swagger(app, config=swagger_config, template=swagger_template)

# Validação de e-mails
def validar_email(email):
    padrao = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(padrao, email)

@app.route('/login', methods=['POST'])
def login():
    """
    Login do usuário com verificação de IP
    ---
    tags:
      - 1. Autenticação
    parameters:
      - in: body
        name: body
        required: true
        description: Endereço de e-mail do usuário
        schema:
          type: object
          properties:
            email:
              type: string
              example: "email_teste@gmail.com"
    responses:
      200:
        description: Token JWT gerado com sucesso
        schema:
          type: object
          properties:
            access_token:
              type: string
              example: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
      403:
        description: IP não autorizado
      404:
        description: Usuário não encontrado ou fora da Security List
      500:
        description: Erro no servidor
    """

    email = request.json.get("email")
    ip_usuario = request.remote_addr  # Captura o IP real do usuário

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Consulta o status e IP do usuário no banco de dados
        cur.execute("""
            SELECT status_whitelist, ip 
            FROM auth.usuarios 
            WHERE email = %s;
        """, (email,))
        usuario = cur.fetchone()

        if not usuario or usuario['status_whitelist'] != "Integrado":
            return jsonify({
                'error': "USUÁRIO NÃO CADASTRADO OU IP FORA DA SECURITY LIST!",
                'message': "Por favor, verifique o status da criação do seu usuário no link abaixo:",
                'link': "https://abrir.me/RzZdr",
                'info': "O job automático de criação de usuários executa hora a hora, todo minuto 30."
            }), 404

        ip_cadastrado = usuario['ip']

        # Verifica se o IP do usuário bate com o IP cadastrado
        if ip_usuario != ip_cadastrado:
            return jsonify({
                'error': "IP NÃO AUTORIZADO!",
                'message': f"Seu IP atual ({ip_usuario}) não corresponde ao IP registrado ({ip_cadastrado}).",
                'info': "Se você mudou de rede, atualize seu IP na planilha de cadastro antes de tentar novamente."
            }), 403

        # Gera um token JWT se o IP estiver correto
        access_token = create_access_token(identity=email)
        return jsonify({'access_token': access_token}), 200

    except Exception as e:
        return jsonify({'error': f"Erro ao autenticar: {e}"}), 500
    finally:
        cur.close()
        conn.close()


# Rota para listar tabelas
@app.route('/tabelas', methods=['GET'])
@jwt_required()
def listar_tabelas():
    """
    Listar tabelas disponíveis
    ---
    tags:
      - 2. Tabelas
    parameters:
      - in: header
        name: Authorization
        type: string
        required: true
        description: Token JWT no formato "Bearer <token>"
    responses:
      200:
        description: |
          Lista de tabelas disponíveis.
          Exemplo de uso com curl:
          curl --location 'http://132.226.254.126:5001/tabelas' \\
               --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
        schema:
          type: object
          properties:
            tabelas_disponiveis:
              type: array
              items:
                type: string
              example: ["tabela1", "tabela2"]
      500:
        description: Erro ao listar tabelas
    """


    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'silver_estatisticas_futebol'
            ORDER BY table_name;
        """)
        tabelas = [row['table_name'] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify({'tabelas_disponiveis': tabelas}), 200
    except Exception as e:
        return jsonify({'error': f"Erro ao listar tabelas: {e}"}), 500

# Rota para consultar uma tabela com paginação
@app.route('/tabela/<nome_tabela>', methods=['GET'])
@jwt_required()
def consultar_tabela(nome_tabela):
    """
    Consultar dados de uma tabela com paginação
    ---
    tags:
      - 3. Dados
    parameters:
      - name: nome_tabela
        in: path
        type: string
        required: true
        description: Nome da tabela a ser consultada
      - name: page
        in: query
        type: integer
        required: false
        description: Número da página para paginação
      - in: header
        name: Authorization
        type: string
        required: true
        description: Token JWT no formato "Bearer <token>"
    responses:
      200:
        description: Dados da tabela
      404:
        description: Tabela não encontrada
      500:
        description: Erro ao consultar tabela
    """
    try:
        page = int(request.args.get('page', 1))
        rows_per_page = 100
        offset = (page - 1) * rows_per_page

        conn = get_db_connection()
        cur = conn.cursor()

        # Verifica se a tabela existe
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = %s AND table_schema = 'silver_estatisticas_futebol'
            );
        """, (nome_tabela,))
        if not cur.fetchone()['exists']:
            return jsonify({'error': f"Tabela \"{nome_tabela}\" não encontrada"}), 404

        # Conta o total de linhas na tabela
        cur.execute(f"SELECT COUNT(*) FROM silver_estatisticas_futebol.{nome_tabela};")
        total_rows = cur.fetchone()['count']
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page

        # Consulta os dados da tabela com paginação
        cur.execute(f"SELECT * FROM silver_estatisticas_futebol.{nome_tabela} LIMIT %s OFFSET %s;", (rows_per_page, offset))
        dados = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({
            'tabela': nome_tabela,
            'pagina': page,
            'total_paginas': total_pages,
            'total_linhas': total_rows,
            'dados': dados
        }), 200
    except Exception as e:
        return jsonify({'error': f"Erro ao consultar tabela: {e}"}), 500

# Rota para executar o script Python externo
@app.route('/executar-script', methods=['POST'])
@jwt_required()
def executar_script():
    try:
        # Executa o script externo
        result = subprocess.run(
            ["python3", "/api/integra_security_list.py"],  # Caminho para o script
            capture_output=True, text=True
        )

        # Verifica se o script foi executado com sucesso
        if result.returncode == 0:
            return jsonify({
                'mensagem': "Script executado com sucesso!",
                'saida': result.stdout
            }), 200
        else:
            return jsonify({
                'erro': "Erro ao executar o script.",
                'saida': result.stdout,
                'erro_saida': result.stderr
            }), 500
    except Exception as e:
        return jsonify({'erro': f"Erro ao tentar executar o script: {e}"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
