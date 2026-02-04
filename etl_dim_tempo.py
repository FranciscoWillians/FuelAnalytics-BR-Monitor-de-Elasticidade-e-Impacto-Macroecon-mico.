import pandas as pd
import holidays
from sqlalchemy import create_engine
from datetime import date
from dotenv import load_dotenv
import os


load_dotenv()

#Configuração db
DB_CONNECTION = os.getenv('acesso_db')


"""
    Gera um DataFrame com dados de calendário para a dim_tempo.
    Args:
        start_date: Data inicial 'YYYY-MM-DD'
        end_date: Data final 'YYYY-MM-DD'
"""
def generate_dim_time(start_date: str, end_date: str) -> pd.DataFrame:
    print(f"1. Gerando datas de {start_date} a {end_date}...")
    
    # Gera a sequência de datas diária
    #    A função pandas.date_range é uma ferramenta poderosa na biblioteca Pandas para gerar uma sequência de datas. Ele retorna um #DatetimeIndex de frequência fixa, útil para análise de séries temporais e outras operações relacionadas a datas.

    date = pd.date_range(start = start_date, end = end_date, freq='D')
    df = pd.DataFrame({'data_referencia': date})
    
    # Extrai atributos da data
    df['ano'] = df['data_referencia'].dt.year
    df['mes'] = df['data_referencia'].dt.month
    df['dia'] = df['data_referencia'].dt.day
    df['nome_mes'] = df['data_referencia'].dt.strftime('%B') # Ex: January (podemos traduzir depois)
    df['dia_semana'] = df['data_referencia'].dt.day_name()   # Ex: Monday
    df['trimestre'] = df['data_referencia'].dt.quarter
    
    #calculo semestre
    df['semestre'] = (df['mes'] - 1) // 6 + 1
    
    # --- MAPEAMENTO DE FERIADOS (Brasil) ---
    br_holidays = holidays.Brazil()
    
    # Aplica uma função lambda para verificar se cada data é feriado
    # Se a data estiver na lista de feriados BR, retorna True
    df['e_feriado'] = df['data_referencia'].apply(lambda x: x in br_holidays)
    
    # --- TRADUÇÃO ---
    mapa_meses = {
        'January': 'Janeiro', 'February': 'Fevereiro', 'March': 'Marco', 
        'April': 'Abril', 'May': 'Maio', 'June': 'Junho',
        'July': 'Julho', 'August': 'Agosto', 'September': 'Setembro', 
        'October': 'Outubro', 'November': 'Novembro', 'December': 'Dezembro'
        }
    mapa_dias = {
        'Monday': 'Segunda-feira', 'Tuesday': 'Terca-feira', 'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sabado', 'Sunday': 'Domingo'
    }
    
    df['nome_mes'] = df['nome_mes'].map(mapa_meses)
    df['dia_semana'] = df['dia_semana'].map(mapa_dias)
    
    return df

def load_to_postgres(df: pd.DataFrame, table_name: str):
    """Carrega o DataFrame para o PostgreSQL."""
    try:
        print(f"3. Conectando ao banco de dados...")
        engine = create_engine(DB_CONNECTION)
        
        print(f"4. Inserindo {len(df)} registros na tabela '{table_name}'...")
        # if_exists='append': Adiciona dados. 'replace': Apaga e recria (cuidado!)
        # index=False: Não envia o índice do Pandas (0, 1, 2...) como coluna
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        print("Sucesso! Carga concluída.")
        
    except Exception as e:
        print(f"ERRO CRÍTICO no Banco de Dados: {e}")
        
# execução principal
if __name__ == "__main__":
    # Definindo um range amplo (Passado e Futuro curto)
    
    df_tempo = generate_dim_time('2020-01-01', '2026-12-31')
    
    # Visualizar uma amostra no console antes de salvar
    print(df_tempo.head())
    
    # Salvar no banco
    load_to_postgres(df_tempo, 'dim_tempo')