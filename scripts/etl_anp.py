#esse script baixar os arquivos semestrais da ANP, 


import requests
import pandas as pd
import os
from io import StringIO
from pathlib import Path
import zipfile
import numpy as np
from dotenv import load_dotenv
from zipfile import ZipFile




load_dotenv()

caminho_salvar = os.getenv('pasta_salvamento_de_dados')

def baixar_dados_semestrais_anp(
    ano: int = 2024,
    semestre: int = 1,
    caminho_base: str = caminho_salvar):

   

    print("=" * 60)
    print(f"DOWNLOAD RELATÓRIO {semestre}º SEMESTRE {ano} - ANP")
    print("=" * 60)
     
    
    semestre_str=f"{semestre:02d}"  # 01 ou 02
    

    url = f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{ano}-{semestre_str}.zip"
    
    

    print(f"URL: {url}")
    #pastas

    pasta_destino = Path(caminho_base) / str(ano)

    nome_arquivo = f"relatorio_anp_semestre_{semestre}_{ano}.zip"

    caminho_completo = pasta_destino/nome_arquivo

   

    try:
        # fazendo a requisição no site
        print(f" Conectando à ANP...")
        resposta = requests.get(url, stream=True, timeout=30)
        resposta.raise_for_status()  # Verifica se houve erro HTTP
        #verifica se a pasta existe e se não exitir cria

        pasta_destino.mkdir(parents=True, exist_ok=True)

       

        #tamanho do arquivo
        tamanho_total = int(resposta.headers.get('content-length', 0))
        #download do arquivo

        if tamanho_total:
            tamanho_mb = tamanho_total / (1024 * 1024)
            print(f"📦 Tamanho do arquivo: {tamanho_mb:.1f} MB")
            with open(caminho_completo, 'wb') as arquivo:
                baixado = 0
                bloco_mb = 0
                for chunk in resposta.iter_content(chunk_size=8192):
                    if chunk:
                        arquivo.write(chunk)
                        baixado += len(chunk)

                    # Mostra um ponto a cada ~1MB baixado

                    if baixado // (1024 * 1024) > bloco_mb:
                        print(".", end="", flush=True)
                        bloco_mb += 1
        print()  # Nova linha
        # Verifica se o download foi bem-sucedido

        if caminho_completo.exists():
            tamanho_real = caminho_completo.stat().st_size / (1024 * 1024)
            print(f"\n✅ DOWNLOAD CONCLUÍDO COM SUCESSO!")
            print(f"📁 Arquivo: {caminho_completo}")
            print(f"📊 Tamanho real: {tamanho_real:.1f} MB")
            print(f"📅 Período: {ano} - {semestre}º Semestre")

            return str(caminho_completo)

        else:

            print("❌ ERRO: Arquivo não foi criado")

            return None

    except requests.exceptions.HTTPError as e:

        print(f"\n❌ ERRO HTTP {e.response.status_code}")

        if e.response.status_code == 404:

            print(f"   O relatório ca-{ano}-{semestre_str} não está disponível")

        return None

    except requests.exceptions.Timeout:

        print("\n❌ Tempo de conexão esgotado")

        return None

    except requests.exceptions.ConnectionError:

        print("\n❌ Erro de conexão - verifique sua internet")

        return None

    except Exception as e:

        print(f"\n❌ Erro inesperado: {type(e).__name__}: {e}")

        return None





def verificar_e_baixar(ano=2024, semestre=1):
    """
    Função principal que verifica se já existe o arquivo e baixa se necessário.
    """

    # caminho para salvar

    CAMINHO_BASE = os.getenv('pasta_salvamento_de_dados')

    # Caminho esperado do arquivo
    pasta = Path(CAMINHO_BASE) / str(ano)
    arquivo_esperado = pasta / f"relatorio_anp_semestre_{semestre}_{ano}.zip"
    
    # Verifica se o arquivo já existe

    if arquivo_esperado.exists():

        tamanho = arquivo_esperado.stat().st_size / (1024 * 1024)
        print(f"📁 Arquivo já existe: {arquivo_esperado}")
        print(f"📊 Tamanho: {tamanho:.1f} MB")
        resposta = input("\n📝 Deseja baixar novamente? (s/n): ")

        if resposta.lower() != 's':
            print("✅ Usando arquivo existente")
            return str(arquivo_esperado)

    # Se não existe ou usuário quer baixar novamente

    return baixar_dados_semestrais_anp(
        ano=ano,
        semestre=semestre,
        caminho_base=CAMINHO_BASE
    )


def extrair_arquivos_zip(arquivo_zip):
    try:
        with ZipFile (arquivo_zip,"r") as zip_file:
            file_names = zip_file.namelist()
            print(f"📦 Arquivos no ZIP: {len(file_names)}")
            pasta_destino = Path("arquivo_zip_primeiro_semestre")
            pasta_destino.mkdir(exist_ok=True)
            print(f"📂 Extraindo para: {pasta_destino.absolute()}")
            # Contador de arquivos CSV extraídos
            arquivos_csv = 0
            #Extrair cada arquivo individualmente
            for i, file_name in enumerate(file_names, 1):
                if file_name.lower().endswith('.csv'):
                    print(f"  {i:3d}. 📄 {file_name}")
                    zip_file.extract(file_name, pasta_destino)
                    arquivos_csv += 1
                    print(f"\n✅ Concluído! {arquivos_csv} arquivos CSV extraídos.")
                    return pasta_destino
    except Exception as e:
                print(f"❌ Erro na extração: {type(e).__name__}: {e}")
                return None
    




def main():
    """
    Função principal para executar o pipeline completo.
    
    CORREÇÃO: Reorganizado o fluxo lógico:
    1. Baixar dados (se necessário)
    2. Extrair arquivos
    3. Processar dados (a implementar)
    """
    
    print("=" * 70)
    print("🚀 PIPELINE ETL - DADOS DA ANP")
    print("=" * 70)
    
    # ETAPA 1: DOWNLOAD DOS DADOS
    print("\n📥 ETAPA 1: DOWNLOAD DOS DADOS")
    
    # CORREÇÃO: Usar a função de verificação que verifica se já existe
    arquivo_zip = verificar_e_baixar(ano=2024, semestre=1)
    
    if not arquivo_zip:
        print("❌ Falha no download. Encerrando.")
        return
    
    print(f"\n✅ Download concluído: {arquivo_zip}")
    
    # ETAPA 2: EXTRAÇÃO DOS ARQUIVOS
    print("\n📂 ETAPA 2: EXTRAÇÃO DOS ARQUIVOS")
    
    # CORREÇÃO: Chamar função de extração com o caminho correto
    pasta_extraidos = extrair_arquivos_zip(arquivo_zip)
    
    if pasta_extraidos:
        print(f"\n✅ Pipeline concluído com sucesso!")
        print(f"📁 Dados extraídos em: {pasta_extraidos.absolute()}")
        
        # ETAPA 3: PROCESSAMENTO DOS DADOS (para implementar)
        print("\n💡 Próximos passos (ETAPA 3):")
        print("   1. Carregar os dados CSV no pandas")
        print("   2. Realizar transformações (limpeza, filtros)")
        print("   3. Salvar em formato processado")
        print("   4. Carregar no banco de dados")
    else:
        print("❌ Falha na extração dos dados.")

if __name__ == "__main__":
   
    main()