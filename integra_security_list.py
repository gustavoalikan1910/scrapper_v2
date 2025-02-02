import gspread
from oauth2client.service_account import ServiceAccountCredentials
import oci
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from pytz import timezone

# Configurações do Google Sheets
def get_google_sheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('/api/google/credentials.json', scope)
    client = gspread.authorize(credentials)
    return client.open(sheet_name).sheet1

# Configuração do Oracle OCI
config = oci.config.from_file("~/.oci/config", "DEFAULT")
network_client = oci.core.VirtualNetworkClient(config)
security_list_ocid = "ocid1.securitylist.oc1.sa-saopaulo-1.aaaaaaaavn6rq76wdfna6vilxl22sfuj4tq5lerb3ikn6xox62wsxq5m5nhq"

# Configuração do Banco de Dados
DB_CONFIG = {
    'dbname': 'meu_banco',
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres_db',
    'port': 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# Função para adicionar IP à Security List
def add_ip_to_security_list(ip_address, port=5001):
    try:
        security_list = network_client.get_security_list(security_list_ocid).data
        new_rule = oci.core.models.IngressSecurityRule(
            source=f"{ip_address}/32",
            protocol="6",
            tcp_options=oci.core.models.TcpOptions(
                destination_port_range=oci.core.models.PortRange(min=port, max=port)
            )
        )

        for rule in security_list.ingress_security_rules:
            if rule.source == new_rule.source and rule.tcp_options and rule.tcp_options.destination_port_range.min == port:
                print(f"[LOG] IP {ip_address} já existe na Security List.")
                return True  # Regra já existe

        updated_rules = list(security_list.ingress_security_rules)
        updated_rules.append(new_rule)

        update_details = oci.core.models.UpdateSecurityListDetails(
            ingress_security_rules=updated_rules
        )
        network_client.update_security_list(security_list_ocid, update_details)
        print(f"[LOG] IP {ip_address} adicionado com sucesso à Security List.")
        return True
    except Exception as e:
        print(f"[LOG] Erro ao adicionar IP à Security List: {e}")
        return False

# Função principal de integração
def process_list(sheet_name):
    sheet = get_google_sheet(sheet_name)
    data = sheet.get_all_records()
    sao_paulo_tz = timezone('America/Sao_Paulo')
    data_atual = datetime.now(sao_paulo_tz).strftime('%Y-%m-%d %H:%M:%S')

    conn = get_db_connection()
    cur = conn.cursor()

    for idx, row in enumerate(data, start=2):
        ip = row.get("IP")
        nome = row.get("Nome")
        email = row.get("Email")
        status_whitelist = row.get("StatusSecureList")

        # Verificar se o registro já existe no banco
        cur.execute(
            """
            SELECT nome, email, ip, status_whitelist, data_atualizacao_whitelist 
            FROM auth.usuarios WHERE email = %s
            """,
            (email,)
        )
        existing_user = cur.fetchone()

        if existing_user:
            # Comparar dados e atualizar caso sejam diferentes
            if (
                existing_user["ip"] != ip or
                existing_user["nome"] != nome or
                existing_user["status_whitelist"] != "Integrado"
            ):
                print(f"[LOG] Atualizando registro para o usuário {nome} ({email}).")
                integrado = add_ip_to_security_list(ip)
                status = "Integrado" if integrado else "Não Integrado"

                cur.execute(
                    """
                    UPDATE auth.usuarios
                    SET nome = %s, ip = %s, status_whitelist = %s, 
                        data_atualizacao_whitelist = %s, data_atualizacao = %s
                    WHERE email = %s
                    """,
                    (nome, ip, status, data_atual, data_atual, email)
                )
                conn.commit()

                # Atualizar a planilha
                sheet.update_cell(idx, 4, status)  # Coluna StatusWhiteList
                sheet.update_cell(idx, 5, data_atual)  # Coluna DataAtualizacaoWhiteList
                print(f"[LOG] Registro atualizado para {nome} no banco e na planilha.")
            else:
                print(f"[LOG] Nenhuma atualização necessária para {nome} ({email}).")
        else:
            # Inserir novo registro se não existir
            print(f"[LOG] Inserindo novo registro para {nome} ({email}).")
            integrado = add_ip_to_security_list(ip)
            status = "Integrado" if integrado else "Não Integrado"

            try:
                cur.execute(
                    """
                    INSERT INTO auth.usuarios (nome, email, ip, status_whitelist, data_atualizacao_whitelist, data_atualizacao)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (nome, email, ip, status, data_atual, data_atual)
                )
                conn.commit()

                # Atualizar a planilha
                sheet.update_cell(idx, 4, status)  # Coluna StatusWhiteList
                sheet.update_cell(idx, 5, data_atual)  # Coluna DataAtualizacaoWhiteList
                sheet.update_cell(idx, 6, "Integrado")  # Coluna StatusUsuárioAPI
                sheet.update_cell(idx, 7, data_atual)  # Coluna DataAtualizacaoUsuarioAPI
                print(f"[LOG] Novo usuário {nome} ({email}) inserido com sucesso no banco.")
            except Exception as e:
                print(f"[LOG] Erro ao inserir usuário no banco: {e}")
                conn.rollback()
                sheet.update_cell(idx, 6, "Erro: Não Integrado")  # Coluna StatusUsuárioAPI
                sheet.update_cell(idx, 7, data_atual)  # Coluna DataAtualizacaoUsuarioAPI

    cur.close()
    conn.close()

if __name__ == "__main__":
    print("[LOG] Iniciando o processo de Securitylist...")
    process_list("Security List")
    print("[LOG] Finalizado o processo de Securitylist...")
