import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()
# 1. Carregar apenas o arquivo já pronto e limpo


caminho_dados = os.getenv('arquivo_completo_2024')
df = pd.read_csv(caminho_dados, sep=';', low_memory=False)

# Garantir que a data seja reconhecida como data após a leitura do CSV
df['Data da Coleta'] = pd.to_datetime(df['Data da Coleta'])

print("--- Dados Carregados com Sucesso! ---")

plt.figure(figsize=(10, 6))
df_gasolina = df[df['Produto'] == 'GASOLINA']
df_gasolina.resample('ME', on='Data da Coleta')['Valor de Venda'].mean().plot(kind='line', marker='o')
plt.title('Preço Médio Mensal da Gasolina - 2024')
plt.ylabel('Preço (R$)')
plt.grid(True)
plt.savefig('evolucao_precos.png') # Salva o gráfico como imagem
plt.show()

# Definindo um estilo visual mais moderno
sns.set_theme(style="whitegrid")

plt.figure(figsize=(12, 6))

# Filtrando Gasolina e Etanol
for produto in ['GASOLINA', 'ETANOL']:
    df_temp = df[df['Produto'] == produto]
    resumo_mensal = df_temp.resample('ME', on='Data da Coleta')['Valor de Venda'].mean()
    plt.plot(resumo_mensal.index.strftime('%b/%Y'), resumo_mensal.values, marker='o', label=produto)

plt.title('Evolução Mensal: Gasolina vs Etanol (2024)', fontsize=14)
plt.ylabel('Preço Médio (R$)')
plt.xlabel('Mês')
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout() # Evita que as legendas cortem na imagem
plt.savefig('comparativo_combustiveis.png')
plt.show()

# --- ANÁLISE 2: TOP 10 CIDADES MAIS CARAS ---
print("\nTop 10 Cidades com Diesel S10 mais caro:")
top_cidades = df[df['Produto'] == 'DIESEL S10'].groupby('Municipio')['Valor de Venda'].mean().nlargest(10)
print(top_cidades)

# --- ANÁLISE 3: MÉDIA POR BANDEIRA ---
resumo_bandeira = df.groupby('Bandeira')['Valor de Venda'].mean().sort_values(ascending=False)
resumo_bandeira.to_sql("relatorio_bandeiras.csv") # Gera um arquivo pequeno de resumo