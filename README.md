⛽ FuelAnalytics BR: Análise de Elasticidade e Impacto Macroeconômico
📊 Visão Geral do Projeto
FuelAnalytics BR é um projeto de análise de dados que investiga a relação entre o preço do petróleo Brent, variáveis macroeconômicas e o preço da gasolina no Brasil. O objetivo é identificar defasagens na transmissão de preços, disparidades regionais e criar um dashboard interativo para monitoramento, servindo como um estudo completo de engenharia e análise de dados.

📁 Fontes de Dados
ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)

O que fornece: Preços de revenda de combustíveis (gasolina, etanol, diesel) por município e posto.

Formato: CSV/XLSX (série histórica semanal).

Link: Dados Abertos ANP

Yahoo Finance (via API yfinance)

O que fornece: Preço do petróleo Brent (ticker BZ=F) e taxa de câmbio USD/BRL (ticker BRL=X).

Frequência: Dados diários.

Biblioteca Python: yfinance

Banco Central do Brasil (SGS - Sistema Gerenciador de Séries Temporais)

O que fornece: Indicadores econômicos como IPCA (inflação), Selic (taxa de juros), PIB, etc.

API: Pode ser acessada via python-bcb ou bacenapi.

Séries importantes:

IPCA: Código 433

Selic: Código 11

Câmbio comercial: Código 1

IPEA (Instituto de Pesquisa Econômica Aplicada)

O que fornece: Dados de importação de derivados de petróleo, produção, etc.

Formato: CSV ou via API.

IBGE (Instituto Brasileiro de Geografia e Estatística)

O que fornece: Dados demográficos e econômicos por município (população, PIB municipal).

Relevante: Para enriquecer a dimensão localidade.

🏗️ Arquitetura Sugerida
Fase 1 - Coleta e Ingestão (Python)
Objetivo: Coletar dados das fontes e salvar em formato bruto.

Estrutura de scripts:

text
scripts/
├── collectors/
│   ├── anp_collector.py      # Baixa dados da ANP
│   ├── yfinance_collector.py # Baixa dados do Yahoo Finance (já existente)
│   └── bcb_collector.py      # Baixa dados do BCB
└── utils/
    ├── logging_config.py     # Configuração de logs
    └── retry_decorator.py    # Decorador para tentativas


Fase 3 - Transformação e Análise (Python + SQL)
Transformações cruciais:

Cálculo de variações percentuais: Compare a variação do Brent com a variação da gasolina.

Correlação e lag: Use a função de correlação cruzada para encontrar o lag (defasagem) que maximiza a correlação.

Elasticidade: Calcule a elasticidade-preço (variação percentual da gasolina / variação percentual do Brent).

Agregações temporais: Média móvel de 7, 14, 30 dias para suavizar ruídos.

Perguntas analíticas complexas:

Qual a defasagem (lag) que maximiza a correlação entre o preço do Brent (em Reais) e o preço médio da gasolina no Brasil?

Existe assimetria na transmissão de preços? (Os aumentos do Brent são repassados mais rapidamente que as quedas?)

Como a elasticidade varia entre as regiões do Brasil?

Qual o impacto da taxa de câmbio e do IPCA no preço final da gasolina?

Há sazonalidade nos preços da gasolina? (ex: alta nas férias, feriados)

Fase 4 - Visualização (Power BI)
Conexão com PostgreSQL:

No Power BI, selecione "Obter Dados" → "Banco de dados" → "PostgreSQL".

Insira o servidor (localhost), banco de dados (gasolina_db) e credenciais.

KPIs e visualizações essenciais:

Preço médio nacional da gasolina (card) e sua variação mensal.

Mapa de calor por estado com o preço médio.

Série temporal comparativa entre Brent (em R$), dólar e gasolina.

Decomposição do preço (estimativa: custo do petróleo, impostos, margem).

Gráfico de lag mostrando a correlação em diferentes defasagens.

Tabela de elasticidade por estado/região.

