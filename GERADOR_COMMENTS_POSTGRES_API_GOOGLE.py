import csv
import time
import google.generativeai as genai

# Configure a API
genai.configure(api_key="AIzaSyB_7iRpDv7bXqDETSgSFtJtJm2kul-c_FI")

# Inicializa o modelo
model = genai.GenerativeModel('gemini-1.5-flash')

#csv_file = 'columns_test.csv'
csv_file = '/home/jovyan/columns_202501042134.csv'
output_file = '/home/jovyan/comentarios.sql'  # Nome do arquivo de saída.  

with open(csv_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
    reader = csv.reader(f_in, delimiter=';', quotechar='"')
    for row in reader:
        # Verifica se a linha tem pelo menos 2 colunas
        if len(row) < 2:
            continue
        
        table_name = row[0]
        column_name = row[1]

        prompt = (
            f"Gera pra mim comentários (Mais direta) em portugues, "
            f"sobre essa coluna. Tabela:{table_name} Coluna:{column_name}. "
            f"Preciso que seja gerado só o COMMENT ON COLUMN do postgre. "
            f"A coluna precisa estar entre aspas duplas no comment. Exemplo \"Blocks_Sh\" "
        )
        
        #print(prompt)
        
        try:
            # Faz a chamada ao modelo
            response = model.generate_content(prompt)

            # Escreve no arquivo de saída
            f_out.write(response.text)
            f_out.write("\n\n")  # Pula linha entre cada comentário
            
        except Exception as e:
            erro_msg = (
                f"-- Erro ao gerar comentário para "
                f"Tabela: {table_name}, Coluna: {column_name}. Erro: {e}\n\n"
            )
            f_out.write(erro_msg)
        
        print(f"Tabela:{table_name} Coluna:{column_name}")

        # Pausa para evitar sobrecarga na API
        time.sleep(5)

print(f"Concluído! Os comentários foram salvos em '{output_file}'.")

