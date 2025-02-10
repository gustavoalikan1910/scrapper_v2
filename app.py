from flask import Flask, request, jsonify, render_template
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


"""
#####################################
ROTAS VISUAIS 
#####################################
"""

@app.route("/")
def home():
    return render_template("index.html")
    
@app.route('/login', methods=['GET'])
def login_page():
    """
    Rota para renderizar a página de login.
    """
    return render_template('login.html')
    
@app.route('/dashboard', methods=['GET'])
def dashboard():
    """
    Rota para renderizar a página de dashboard.
    """
    return render_template('dashboard.html')
    
"""
#####################################
ROTAS VISUAIS 
#####################################
"""

# Configuração Flask-Limiter
limiter = Limiter(key_func=get_remote_address, default_limits=["30 per minute"])
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
        {"name": "1. Autenticação", "description": "Endpoint relacionado à autenticação. Use para testar seu login!"},
        {"name": "2. Tabelas", "description": "Endpoint para listar todas as tabelas disponíveis."},
        {"name": "3. Dados", "description": "Endpoints para consultar os dados das tabelas. Use a paginação."}
    ]
}
Swagger(app, config=swagger_config, template=swagger_template)

# Validação de e-mails
def validar_email(email):
    padrao = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(padrao, email)

# Função de autenticação
def autenticar_usuario():
    email = request.headers.get("email")
    ip_usuario = request.remote_addr  # Captura o IP real do usuário
    
    if not email or not validar_email(email):
        return False, jsonify({"error": "E-mail inválido ou não fornecido."}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT status_whitelist, ip 
            FROM auth.usuarios 
            WHERE email = %s;
        """, (email,))
        usuario = cur.fetchone()
        
        if not usuario or usuario['status_whitelist'] != "Integrado":
            return False, jsonify({
                                'error': "EMAIL NÃO CADASTRADO!",
                                'message': "Por favor, verifique o status da criação do seu usuário no link abaixo:",
                                'link': "https://abrir.me/RzZdr",
                                'info': "O job automático de criação de usuários executa hora a hora, todo minuto 30."
                            }), 403
        
        if ip_usuario != usuario['ip']:
            return False, jsonify({
                                'error': "IP NÃO AUTORIZADO!",
                                'message': f"Seu IP atual ({ip_usuario}) não corresponde ao IP registrado ({ip_cadastrado}).",
                                'info': "Se você mudou de rede, atualize seu IP na planilha de cadastro antes de tentar novamente."
                            }), 403
        
        return True, None, None
    except Exception as e:
        return False, jsonify({"error": f"Erro ao autenticar: {e}"}), 500
    finally:
        cur.close()
        conn.close()

@app.route('/login', methods=['POST'])
def login():
    """
    Rota de login do usuário.
    ---
    tags:
      - 1. Autenticação
    parameters:
      - in: header
        name: email
        type: string
        required: true
        description: Endereço de e-mail do usuário
    responses:
      200:
        description: Login bem-sucedido.
        schema:
          type: object
          properties:
            message:
              type: string
              example: "Login bem-sucedido."
      400:
        description: E-mail não fornecido.
      403:
        description: Email não cadastrado.
      403:
        description: Usuário não autorizado ou IP não permitido.
      500:
        description: Erro interno no servidor.
    """
    autenticado, resposta, status = autenticar_usuario()
    if not autenticado:
        return resposta, status
    return jsonify({"message": "Login bem-sucedido."}), 200


# Rota para listar tabelas
@app.route('/tabelas', methods=['GET'])
def listar_tabelas():
    """
    Listar tabelas disponíveis
    ---
    tags:
      - 2. Tabelas
    parameters:
      - in: header
        name: email
        type: string
        required: true
        description: Endereço de e-mail do usuário
    responses:
      200:
        description: Listagem de todas as tabelas disponiveis para consulta.
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

    autenticado, resposta, status = autenticar_usuario()
        
    if not autenticado:
        return resposta, status
        
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
        name: email
        type: string
        required: true
        description: Endereço de e-mail do usuário
    responses:
      200:
        description: Dados da tabela
      404:
        description: Tabela não encontrada
      500:
        description: Erro ao consultar tabela
    """
    autenticado, resposta, status = autenticar_usuario()
        
    if not autenticado:
        return resposta, status
        
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
def executar_script():
    
    autenticado, resposta, status = autenticar_usuario()
        
    if not autenticado:
        return resposta, status
    try:
        # ✅ Executa o script e captura a saída
        result = subprocess.run(
            ["python3", "/api/integra_security_list.py"],
            capture_output=True, text=True, encoding="utf-8"
        )

        # ✅ Divide os logs em uma lista de strings (melhor para o Postman)
        stdout_lines = result.stdout.strip().split("\n") if result.stdout else []
        stderr_lines = result.stderr.strip().split("\n") if result.stderr else []

        # ✅ Se o script foi executado com sucesso (código 0)
        if result.returncode == 0:
            return jsonify({
                "mensagem": "Script executado com sucesso!",
                "logs": stdout_lines  # Agora os logs são uma lista, mais fácil de visualizar no Postman
            }), 200
        else:
            return jsonify({
                "erro": "Erro ao executar o script.",
                "logs": stdout_lines,
                "erro_saida": stderr_lines
            }), 500

    except Exception as e:
        return jsonify({
            "erro": f"Erro ao tentar executar o script: {str(e)}"
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
