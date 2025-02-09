import gspread
from oauth2client.service_account import ServiceAccountCredentials
import oci
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from pytz import timezone

# 🔹 Configurações do Google Sheets
def get_google_sheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('/api/google/credentials.json', scope)
    client = gspread.authorize(credentials)
    return client.open(sheet_name).sheet1

# 🔹 Configuração do Oracle OCI
config = oci.config.from_file("/api/oci/config", "DEFAULT")
network_client = oci.core.VirtualNetworkClient(config)
security_list_ocid = "ocid1.securitylist.oc1.sa-saopaulo-1.aaaaaaaavn6rq76wdfna6vilxl22sfuj4tq5lerb3ikn6xox62wsxq5m5nhq"

# 🔹 Configuração do Banco de Dados
DB_CONFIG = {
    'dbname': 'meu_banco',
    'user': 'admin',
    'password': 'admin',
    'host': 'postgres_db',
    'port': 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# 🔹 Função para obter IPs existentes na Security List da Oracle Cloud
def get_existing_ips_from_security_list():
    try:
        security_list = network_client.get_security_list(security_list_ocid).data
        return {rule.source.replace("/32", "") for rule in security_list.ingress_security_rules}
    except Exception as e:
        print(f"[LOG] Erro ao obter IPs da Security List: {e}")
        return set()

# 🔹 Função para obter IPs existentes no PostgreSQL
def get_existing_ips_from_postgres():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT ip FROM auth.usuarios;")
        existing_ips = {row["ip"] for row in cur.fetchall()}
        conn.close()
        return existing_ips
    except Exception as e:
        print(f"[LOG] Erro ao obter IPs do PostgreSQL: {e}")
        return set()

# 🔹 Função para adicionar IP à Security List da Oracle Cloud para portas específicas (ex: 5001 e 9002)
def add_ip_to_security_list(ip_address, ports=[5001, 9002]):
    try:
        security_list = network_client.get_security_list(security_list_ocid).data
        updated_rules = list(security_list.ingress_security_rules)  # Copia as regras existentes

        for port in ports:
            new_rule = oci.core.models.IngressSecurityRule(
                source=f"{ip_address}/32",
                protocol="6",  # TCP
                tcp_options=oci.core.models.TcpOptions(
                    destination_port_range=oci.core.models.PortRange(min=port, max=port)
                )
            )

            # Verifica se já existe essa regra antes de adicionar
            if any(rule.source == new_rule.source and 
                   rule.tcp_options and 
                   rule.tcp_options.destination_port_range.min == port 
                   for rule in security_list.ingress_security_rules):
                print(f"[LOG] IP {ip_address} já tem permissão para a porta {port}.")
            else:
                updated_rules.append(new_rule)
                print(f"[LOG] IP {ip_address} adicionado para a porta {port}.")

        # Atualiza a Security List com as novas regras
        update_details = oci.core.models.UpdateSecurityListDetails(
            ingress_security_rules=updated_rules
        )
        network_client.update_security_list(security_list_ocid, update_details)

        return True

    except Exception as e:
        print(f"[LOG] Erro ao adicionar IP {ip_address} à Security List: {e}")
        return False

# 🔹 Função para liberar todas as portas para um IP (utilizada para o email gu.alikan@gmail.com)
def add_ip_all_ports_to_security_list(ip_address):
    try:
        security_list = network_client.get_security_list(security_list_ocid).data
        updated_rules = list(security_list.ingress_security_rules)

        # Cria uma regra para liberar todas as portas (TCP de 1 a 65535)
        new_rule = oci.core.models.IngressSecurityRule(
            source=f"{ip_address}/32",
            protocol="6",  # TCP
            tcp_options=oci.core.models.TcpOptions(
                destination_port_range=oci.core.models.PortRange(min=1, max=65535)
            )
        )

        # Verifica se já existe essa regra para o IP
        rule_exists = any(
            rule.source == new_rule.source and
            rule.tcp_options and
            rule.tcp_options.destination_port_range.min == 1 and 
            rule.tcp_options.destination_port_range.max == 65535
            for rule in security_list.ingress_security_rules
        )

        if rule_exists:
            print(f"[LOG] IP {ip_address} já possui liberação para todas as portas.")
        else:
            updated_rules.append(new_rule)
            print(f"[LOG] IP {ip_address} liberado para todas as portas.")
            update_details = oci.core.models.UpdateSecurityListDetails(
                ingress_security_rules=updated_rules
            )
            network_client.update_security_list(security_list_ocid, update_details)

        return True

    except Exception as e:
        print(f"[LOG] Erro ao liberar todas as portas para o IP {ip_address}: {e}")
        return False

# 🔹 Função para remover IPs antigos da Security List APENAS para as portas 5001 e 9002
def remove_old_ips_from_security_list(valid_ips):
    try:
        print("[LOG] Iniciando remoção de IPs antigos da Security List (apenas portas 5001 e 9002)...")

        security_list = network_client.get_security_list(security_list_ocid).data
        current_rules = security_list.ingress_security_rules

        # Garante que os valid_ips estejam sem o sufixo /32
        valid_ips = {ip.replace("/32", "") for ip in valid_ips}

        target_ports = {5001, 9002}

        # Identifica os IPs que devem ser removidos somente para as portas alvo
        ips_to_remove = {
            rule.source.replace("/32", "")
            for rule in current_rules
            if rule.tcp_options and rule.tcp_options.destination_port_range.min in target_ports
            and rule.source.replace("/32", "") not in valid_ips
        }

        if not ips_to_remove:
            print("[LOG] Nenhum IP desatualizado para remover nas portas 5001 e 9002.")
            return

        print(f"[LOG] Removendo os seguintes IPs para portas 5001 e 9002: {ips_to_remove}")

        updated_rules = [
            rule for rule in current_rules
            if not (rule.tcp_options and rule.tcp_options.destination_port_range.min in target_ports 
                    and rule.source.replace("/32", "") in ips_to_remove)
        ]

        update_details = oci.core.models.UpdateSecurityListDetails(
            ingress_security_rules=updated_rules
        )
        network_client.update_security_list(security_list_ocid, update_details)

        print("[LOG] IPs antigos removidos da Security List com sucesso.")
    except Exception as e:
        print(f"[LOG] Erro ao remover IPs da Security List: {e}")

# 🔹 Função para remover IPs antigos do PostgreSQL
def remove_old_ips_from_postgres(valid_ips):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM auth.usuarios WHERE ip NOT IN %s;", (tuple(valid_ips),))
        conn.commit()
        conn.close()
        print("[LOG] IPs antigos removidos do PostgreSQL.")
    except Exception as e:
        print(f"[LOG] Erro ao remover IPs do PostgreSQL: {e}")

# 🔹 Função principal de integração
def process_list(sheet_name):
    sheet = get_google_sheet(sheet_name)
    data = sheet.get_all_records()
    sao_paulo_tz = timezone('America/Sao_Paulo')
    data_atual = datetime.now(sao_paulo_tz).strftime('%Y-%m-%d %H:%M:%S')

    conn = get_db_connection()
    cur = conn.cursor()

    ip_list_from_sheet = {row["IP"] for row in data if row.get("IP")}
    valid_ips = ip_list_from_sheet

    for idx, row in enumerate(data, start=2):
        ip = row.get("IP")
        nome = row.get("Nome")
        email = row.get("Email")
        status_whitelist = row.get("StatusSecureList")

        # Se o email for gu.alikan@gmail.com, liberar todas as portas
        if email.lower() == "gu.alikan@gmail.com":
            integrado = add_ip_all_ports_to_security_list(ip)
            status = "Integrado" if integrado else "Não Integrado"
            
            cur.execute("SELECT nome, email, ip FROM auth.usuarios WHERE email = %s", (email,))
            existing_user = cur.fetchone()

            if existing_user:
                cur.execute(
                    "UPDATE auth.usuarios SET nome = %s, ip = %s, status_whitelist = %s, data_atualizacao_whitelist = %s, data_atualizacao = %s WHERE email = %s",
                    (nome, ip, status, data_atual, data_atual, email)
                )
                conn.commit()
                sheet.update_cell(idx, 4, status)
                sheet.update_cell(idx, 5, data_atual)
                sheet.update_cell(idx, 6, "Integrado")
                sheet.update_cell(idx, 7, data_atual)
            else:
                cur.execute(
                    "INSERT INTO auth.usuarios (nome, email, ip, status_whitelist, data_atualizacao_whitelist, data_atualizacao) VALUES (%s, %s, %s, %s, %s, %s)",
                    (nome, email, ip, status, data_atual, data_atual)
                )
                conn.commit()
                sheet.update_cell(idx, 4, status)
                sheet.update_cell(idx, 5, data_atual)
                sheet.update_cell(idx, 6, "Integrado")
                sheet.update_cell(idx, 7, data_atual)
        else:
            # Lógica padrão para os demais emails
            cur.execute("SELECT nome, email, ip FROM auth.usuarios WHERE email = %s", (email,))
            existing_user = cur.fetchone()

            if existing_user:
                if existing_user["ip"] != ip or existing_user["nome"] != nome:
                    integrado = add_ip_to_security_list(ip)
                    status = "Integrado" if integrado else "Não Integrado"
                    cur.execute(
                        "UPDATE auth.usuarios SET nome = %s, ip = %s, status_whitelist = %s, data_atualizacao_whitelist = %s, data_atualizacao = %s WHERE email = %s",
                        (nome, ip, status, data_atual, data_atual, email)
                    )
                    conn.commit()
                    sheet.update_cell(idx, 6, "Integrado")
                    sheet.update_cell(idx, 7, data_atual)
            else:
                integrado = add_ip_to_security_list(ip)
                status = "Integrado" if integrado else "Não Integrado"
                cur.execute(
                    "INSERT INTO auth.usuarios (nome, email, ip, status_whitelist, data_atualizacao_whitelist, data_atualizacao) VALUES (%s, %s, %s, %s, %s, %s)",
                    (nome, email, ip, status, data_atual, data_atual)
                )
                conn.commit()
                sheet.update_cell(idx, 4, status)
                sheet.update_cell(idx, 5, data_atual)
                sheet.update_cell(idx, 6, "Integrado")
                sheet.update_cell(idx, 7, data_atual)

    cur.close()
    conn.close()

    remove_old_ips_from_security_list(valid_ips)
    remove_old_ips_from_postgres(valid_ips)

if __name__ == "__main__":
    print("[LOG] Iniciando o processo de Security List...")
    process_list("Security List")
    print("[LOG] Finalizado o processo de Security List...")
