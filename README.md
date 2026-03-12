# 📊 Case de Estágio Cobli - Análise de Acidentes Rodoviários (PRF)

## 📝 Descrição do Projeto
Este projeto foi desenvolvido como parte do case técnico para a vaga de estágio na **Cobli**. O objetivo principal é processar, limpar, padronizar e extrair insights dos dados abertos da Polícia Rodoviária Federal (PRF) referentes aos acidentes rodoviários ocorridos no Brasil entre **01/01/2020 e 31/12/2023**. 

O pipeline de dados foi construído utilizando a **Arquitetura Medalhão (Bronze, Prata e Ouro)** dentro do ambiente **Databricks**, utilizando **PySpark** e **Spark SQL** para processamento em larga escala. No final do processo, os dados foram filtrados para o estado de São Paulo (SP) e salvos como uma **Delta Table** para otimização de consultas analíticas. 

Todo o processo foi automatizado para rodar diariamente utilizando o **Databricks Workflows com um Trigger para rodar todo dia **.

---

## 🛠️ Tecnologias e Bibliotecas Utilizadas
* **Ambiente:** Databricks Free Community Edition
* **Linguagem:** Python 3 e SQL
* **Framework:** Apache Spark (PySpark) e Delta Lake
* **Orquestração:** Databricks Workflows (Jobs & Triggers)
* **Versionamento:** Git e GitHub
* **Bibliotecas Nativas (PySpark):** 
  * `col`, `to_date`, `count`, `when` (manipulação e limpeza de dados)

---

## ⚙️ Como executar este repositório no Databricks

Como este código foi desenvolvido para rodar no Databricks, siga o passo a passo abaixo para reproduzi-lo:

1. **Clone o repositório:**
   * Conecte seu GitHub ao Databricks via *Repos* ou baixe o notebook `.ipynb` deste repositório e importe no seu *Workspace* do Databricks.
2. **Upload da Base de Dados:**
   * Baixe os dados da PRF (2020 a 2023) no portal de dados abertos do governo.
   * No Databricks, vá em **Catalog > Volumes** e crie a seguinte estrutura de pastas: `/Volumes/workspace/default/dados_brutos/`.
   * Faça o upload do arquivo CSV com o nome `acidentes_brasil.csv`.
3. **Inicie o Cluster:**
   * Certifique-se de ter um cluster ativo (Compute) no Databricks.
   * Anexe o notebook ao cluster.
4. **Execução Manual ou Automatizada:**
   * **Manual:** Rode o notebook de cima para baixo (botão *Run All*).
   * **Automática:** O projeto possui um agendamento configurado via *Databricks Workflows* para rodar a pipeline automaticamente.

---

## 🧹 Pipeline de Dados e Tomadas de Decisão

Abaixo está o detalhamento do pipeline, as transformações realizadas em cada camada e o motivo de cada escolha técnica:

### 1. Ingestão (Camada Bronze)
Os dados foram lidos indicando o separador `;` e encoding `utf-8`.

### 2. Padronização de Cabeçalhos (Camada Prata)
Todos os nomes de colunas foram convertidos para letras minúsculas e os espaços foram substituídos por _underline_ (`_`).

### 3. Tipagem de Dados (String ➡️ Date)
A coluna `data_inversa` foi renomeada para `data` e convertida do tipo `String` para `Date`.

### 4. Tratamento de Nulos e Duplicatas
* `dropna(how='all')` e `dropDuplicates()`: Remoção de linhas vazias e duplicadas.

### 5. Imputação de Valores "NULL"
Substituição de strings escritas como "NULL" pelo termo "N/I" (Não Informado).

### 6. Filtro Regional (São Paulo)
Criação de um Dataframe focado nos acidentes onde `uf = 'SP'`.

### 7. Armazenamento e Analytics (Camada Ouro e Delta Lake)
Os dados tratados de São Paulo foram salvos fisicamente em uma tabela no formato **Delta** (`default.tabela_acidentes_sp`).
O formato Delta Lake traz suporte a transações ACID, versionamento de dados (Time Travel) e altíssima performance de leitura para ferramentas de BI ou para execução de Queries SQL analíticas. A Camada Ouro representa o dado final, pronto para responder perguntas de negócio.

### 8. Orquestração e Automação (Databricks Workflows) ⏱️
Foi criado um **Job / Trigger** no Databricks Workflows agendado para rodar essa pipeline de dados **uma vez ao dia**.
* **Por que?** Em um cenário real de negócios, dados novos chegam constantemente. Automatizar a pipeline garante a **disponibilidade contínua (data freshness)**. Para uma empresa de gestão de frotas e logística como a Cobli, ter tabelas atualizadas diariamente de forma autônoma significa que os dashboards e modelos de risco sempre refletirão a realidade mais recente, sem depender de execução manual de um Engenheiro de Dados.

---

## 🔍 Principais Códigos e Insights (SQL)

df_ouro = spark.read.table("default.tabela_acidentes_sp")

df_ouro.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.tabela_acidentes_sp") 
    2. Insights de Negócio com Spark SQL (%sql):

A partir da Camada Ouro, foi possível extrair KPIs.

Evolução de Acidentes ao Longo do Tempo: Permite identificar sazonalidades (ex: feriados, época de chuvas).
SELECT data, COUNT(*) as qtd_acidentes
FROM tabela_acidentes_sp
GROUP BY data
ORDER BY data;

SELECT COUNT(*) as total_acidentes
FROM tabela_acidentes_sp;

Análise de Risco por Dia da Semana (Sexta-feira): Sextas-feiras costumam apresentar tráfego intenso devido à saída para o final de semana e rush logístico. Entender a volumetria neste dia ajuda empresas como a Cobli a planejar roteirizações mais seguras para seus motoristas.

SELECT dia_semana, COUNT(*) as total_acidentes_na_sexta
FROM tabela_acidentes_sp
WHERE dia_semana = 'sexta-feira'
GROUP BY dia_semana;
