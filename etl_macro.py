import yfinance as yf
import pandas as pd

def get_market_data(ticker, start_date): #ticker = indice que quero buscar informações
    """
    Busca dados históricos do Yahoo Finance.
    Ex: ticker='BZ=F' para Brent Crude Oil
    """
    print(f"Extraindo dados para {ticker}...") # f antes das aspas é para  py reconhecer o que está entre chaves
    data = yf.download(ticker, start=start_date)
    # Reset index para transformar a Data de índice para coluna
    return data.reset_index()

# Exemplo de uso
df_brent = get_market_data('BZ=F', '2023-01-01')
df_dolar = get_market_data('BRL=X', '2023-01-01')