import pandas as pd
from pathlib import Path
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

caminho_csv =Path(os.getenv('arquivo_csv_2024'))

arquivos_csv = list(caminho_csv.glob("*.csv"))


lista_dataframes = []

print(f"Encontrados {len(arquivos_csv)} arquivos. Iniciando processamento...")

for caminho_arquivo in arquivos_csv:
    
    print(f"Lendo: {caminho_arquivo.name}")
    
    df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', low_memory=False)
    
# Tratamentos básicos antes de unir
    df['Valor de Venda'] = df['Valor de Venda'].str.replace(',', '.').astype(float)
    df['Data da Coleta'] = pd.to_datetime(df['Data da Coleta'], dayfirst=True)
    if df['Valor de Compra'].isna().all():
        df = df.drop(columns=['Valor de Compra'])
        
    # Adicionando o dataframe na lista
    lista_dataframes.append(df)



if lista_dataframes:
    
    # Junta tudo em um único objeto
    df_anual = pd.concat(lista_dataframes, ignore_index=True)
    print(f"✅ Sucesso! Total de registros no ano: {len(df_anual)}")
    
    nome_saida = caminho_csv / "Precos_ANP_2024_Completo.csv"
    df_anual.to_csv(nome_saida, index=False, sep=';', encoding='utf-8-sig')
    print(f"💾 Arquivo consolidado salvo em: {nome_saida}")
else:
    print("❌ Nenhum arquivo CSV encontrado para processar.")
    
    

