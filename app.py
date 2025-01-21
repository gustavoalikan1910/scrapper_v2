from flask import Flask, request, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import uuid
from datetime import datetime
from pytz import timezone  # Importa pytz para lidar com fusos horários

app = Flask(__name__)

# Configuração JWT
app.config["JWT_SECRET_KEY"] = "f792048144e5b4595f10ae9fe02729b4082cf7fa97883be5ae3216a736098d44"
jwt = JWTManager(app)

# Configuração Flask-Limiter
limiter = Limiter(key_func=get_remote_address)
limiter.init_app(app)


# Configuração PostgreSQL
DB_CONFIG = {
    'dbname': 'meu_banco',
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres_db',
    'port': 5432
}

# Função para conectar ao banco de dados
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# Validação de e-mails
def validar_email(email):
    padrao = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(padrao, email)

# Endpoint para registrar usuário
@app.route('/registrar', methods=['POST'])
@limiter.limit("5 per minute")  # Limita a 5 registros por minuto por IP
def registrar_usuario():
    nome = request.json.get('nome')
    email = request.json.get('email')

    if not nome or len(nome) < 3:
        return jsonify({'error': 'Nome é obrigatório e deve ter pelo menos 3 caracteres'}), 400

    if not validar_email(email):
        return jsonify({'error': 'E-mail inválido'}), 400

    token = str(uuid.uuid4())  # Gera um token único
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Define o fuso horário de São Paulo
        sao_paulo_tz = timezone('America/Sao_Paulo')
        data_atual = datetime.now(sao_paulo_tz)  # Obtém a data/hora no fuso de São Paulo

        # Verifica se o e-mail já está registrado
        cur.execute("SELECT COUNT(*) FROM auth.usuarios WHERE email = %s;", (email,))
        if cur.fetchone()['count'] > 0:
            return jsonify({'error': 'E-mail já registrado'}), 409

        # Insere o usuário no banco
        cur.execute(
            "INSERT INTO auth.usuarios (nome, email, token, data_criacao, data_atualizacao) VALUES (%s, %s, %s, %s, %s) RETURNING token;",
            (nome, email, token, data_atual, data_atual)
        )
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'nome': nome, 'email': email, 'token': token}), 201
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({'error': f'Erro ao registrar usuário: {e}'}), 500

# Endpoint para atualizar usuário
@app.route('/usuarios/<int:user_id>', methods=['PUT'])
@jwt_required()
def atualizar_usuario(user_id):
    novos_dados = request.json
    nome = novos_dados.get('nome')
    email = novos_dados.get('email')

    if not nome or len(nome) < 3:
        return jsonify({'error': 'Nome é obrigatório e deve ter pelo menos 3 caracteres'}), 400

    if not validar_email(email):
        return jsonify({'error': 'E-mail inválido'}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Atualizar nome, e-mail e data_atualizacao
        query = """
            UPDATE auth.usuarios
            SET nome = %s, email = %s, data_atualizacao = %s
            WHERE id = %s;
        """
        cur.execute(query, (nome, email, datetime.now(), user_id))
        conn.commit()

        if cur.rowcount == 0:
            return jsonify({'error': 'Usuário não encontrado'}), 404

        cur.close()
        conn.close()
        return jsonify({'message': 'Usuário atualizado com sucesso'}), 200
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({'error': f'Erro ao atualizar usuário: {e}'}), 500

# Endpoint para login
@app.route('/login', methods=['POST'])
def login():
    email = request.json.get("email")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Valida se o e-mail existe
        cur.execute("SELECT token FROM auth.usuarios WHERE email = %s;", (email,))
        usuario = cur.fetchone()

        if not usuario:
            return jsonify({'error': 'Usuário não encontrado'}), 404

        # Gera um token JWT
        access_token = create_access_token(identity=email)
        return jsonify({'access_token': access_token}), 200
    except Exception as e:
        return jsonify({'error': f'Erro ao autenticar: {e}'}), 500
    finally:
        cur.close()
        conn.close()

# Endpoint para listar tabelas
@app.route('/tabelas', methods=['GET'])
@jwt_required()
def listar_tabelas():
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
        return jsonify({'error': f'Erro ao listar tabelas: {e}'}), 500

# Endpoint para consultar uma tabela com paginação
@app.route('/tabela/<nome_tabela>', methods=['GET'])
@jwt_required()
def consultar_tabela(nome_tabela):
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
            return jsonify({'error': f'Tabela "{nome_tabela}" não encontrada'}), 404

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
        return jsonify({'error': f'Erro ao consultar tabela: {e}'}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
