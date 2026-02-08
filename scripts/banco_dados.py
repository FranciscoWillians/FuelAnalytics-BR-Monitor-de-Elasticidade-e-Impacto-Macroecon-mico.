import pandas as pd # <--- Você precisa importar o pandas aqui também
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

# --- PASSO NOVO: Carregar os dados que foram salvos pelo outro script ---
# Use o caminho do arquivo consolidado que você definiu no seu .env
caminho_consolidado = os.getenv('arquivo_completo_2024') 

print(f"📖 Lendo o arquivo para envio: {caminho_consolidado}")
df_anual = pd.read_csv(caminho_consolidado, sep=';', low_memory=False)
# -----------------------------------------------------------------------

# Pegando os dados de conexão do seu .env
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
database = os.getenv('DB_NAME')

# Criando a conexão
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# Enviando o DataFrame para o banco
print("📤 Enviando dados para o PostgreSQL...")

# Tratamento dos nomes das colunas
df_anual.columns = [c.lower()
    .replace(' - ', '_')
    .replace(' ', '_')
    .replace('-', '_')
    .replace('__', '_') for c in df_anual.columns]

try:
    df_anual.to_sql('precos_anp_2024', engine, if_exists='replace', index=False)
    # Criando a Dimensão Produto
    dim_produto = df_anual[['produto', 'unidade_de_medida']].drop_duplicates().reset_index(drop=True)
    dim_produto['id_produto'] = dim_produto.index
    # Criando a Dimensão Posto
    dim_posto = df_anual[['cnpj_da_revenda', 'revenda', 'bandeira', 'municipio', 'estado_sigla', 'regiao_sigla']].drop_duplicates().reset_index(drop=True)
    dim_posto['id_posto'] = dim_posto.index
    
    # Criando a Fato (Substituindo os textos pelos IDs)
    fato_precos = df_anual.merge(dim_produto, on=['produto', 'unidade_de_medida']) \
        .merge(dim_posto, on=['cnpj_da_revenda', 'revenda', 'bandeira', 'municipio', 'estado_sigla', 'regiao_sigla'])
        
        # Agora você só precisa das colunas de ID e o valor
    fato_precos = fato_precos[['data_da_coleta', 'valor_de_venda', 'id_produto', 'id_posto']]
        
        # Enviando cada uma para o PostgreSQL
    dim_produto.to_sql('dim_produto', engine, if_exists='replace', index=False)
    dim_posto.to_sql('dim_posto', engine, if_exists='replace', index=False)
    fato_precos.to_sql('fato_precos', engine, if_exists='replace', index=False)
    print("✅ Dados carregados no banco com sucesso!")
except Exception as e:
    
    print(f"❌ Erro na conexão: {e}")