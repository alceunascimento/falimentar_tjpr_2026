# falimentar_tjpr_2026

**Análise Empírica de Processos Falimentares no TJPR**  
Pipeline de extração, parsing e análise de dados judiciais via API DataJud (CNJ)

> Observatório do Poder Judiciário — OAB Paraná  
> Autor: Alceu Eilert Nascimento — `alceuenascimento@gmail.com`

---

## Descrição

Pipeline de dados end-to-end para coleta, processamento e análise de processos falimentares e de recuperação de empresas no Tribunal de Justiça do Paraná (TJPR), com dados obtidos via **API DataJud** (CNJ — Conselho Nacional de Justiça).

O pipeline é integralmente implementado em **R**, compreendendo três scripts sequenciais: extração paralela via API REST, parsing via DuckDB com exportação em Parquet, e carga/limpeza com enriquecimento geográfico via API IBGE.

---

## Classes Processuais Monitoradas

| Código CNJ | Classe Processual          |
|:----------:|:---------------------------|
| 108        | Falência                   |
| 111        | Recuperação Judicial       |
| 114        | Recuperação Extrajudicial  |
| 128        | Habilitação de Crédito     |
| 129        | Impugnação de Crédito      |

---

## Arquitetura do Pipeline

```
DataJud API (CNJ)
      │
      ▼
[1. extract]  datajud_obter_dados_classe_paralelo_windows.R
              Requisições paralelas por classe processual (furrr/future)
              Saída: data/*.ndjson  (um arquivo por classe × timestamp)
      │
      ▼
[2. parse]    datajud_parse.R
              NDJSON → DuckDB (in-memory) → Parquet colunar
              Saída: data/parsed/{processos,movimentos,assuntos}.parquet
      │
      ▼
[3. load]     datajud_load_n_clean_logger.R
              Parquet → R (duckdb) → limpeza → enriquecimento IBGE
              Saída: data/cleaned/{df_processos,df_movimentos,df_assuntos}.rds
                     (ou dataframes no ambiente R se executado via source())
      │
      ▼
[4. analysis] scripts em analysis/
              Análise de sobrevivência, distribuições temporais,
              mapas coropléticos, modelos econométricos
              Saída: output/*.csv, output/*.html
```

---

## Estrutura de Diretórios

```
falimentar_tjpr_2026/
├── bin/
│   ├── datajud_obter_dados_classe_paralelo_windows.R    # [1] extração
│   ├── datajud_parse.R                                  # [2] parsing
│   └── datajud_load_n_clean_logger.R                    # [3] carga e limpeza
├── analysis/
│   └── *.R                                              # análises e modelos
├── data/
│   ├── *.ndjson        # brutos da API DataJud  ← ignorados pelo git
│   ├── parsed/
│   │   ├── processos.parquet   ← ignorado pelo git
│   │   ├── movimentos.parquet  ← ignorado pelo git
│   │   └── assuntos.parquet    ← ignorado pelo git
│   └── cleaned/
│       ├── df_processos.rds    ← ignorado pelo git
│       ├── df_movimentos.rds   ← ignorado pelo git
│       └── df_assuntos.rds     ← ignorado pelo git
├── logs/
│   └── datajud_load_n_clean_YYYYMMDD_HHMMSS.log
├── output/
│   └── *.csv
├── .gitignore
├── falimentar_tjpr_2026.Rproj
└── README.md
```

> **Nota:** Arquivos `*.ndjson` e `*.rds` são excluídos do versionamento via `.gitignore`. Os arquivos brutos atingem centenas de MB a ~1 GB por classe processual; os RDS de movimentos podem superar 80 MB. Apenas os scripts, outputs CSV são versionados.

---

## Scripts

### `datajud_obter_dados_classe_paralelo_windows.R` — Extração

Recebe `tribunal` e um ou mais `classe_codigo` como argumentos de linha de comando. Para cada classe, executa requisições paginadas à API DataJud com `search_after` (scroll via sort cursor), coletando até `max_pag × 10.000` registros por classe. A execução é paralelizada via `furrr::future_map()` com backend `multisession`, usando `min(n_classes, availableCores() - 1)` workers. Cada classe é serializada em NDJSON individual com timestamp no nome.

```bash
Rscript bin/datajud_obter_dados_classe_paralelo_windows.R tjpr 108 111 114 128 129
```

**Dependências:** `httr`, `tidyverse`, `glue`, `jsonlite`, `future`, `furrr`, `usethis`

---

### `datajud_parse.R` — Parsing NDJSON → Parquet

Detecta todos os arquivos `datajud_raw_*.ndjson` em `data/`, inicializa uma sessão DuckDB in-memory (configurada para 45 GB de RAM e 4 threads) e executa três queries de criação de tabela com `read_json_auto()` e `UNNEST()` para normalização das listas aninhadas (`assuntos`, `movimentos`, `complementosTabelados`). As tabelas resultantes são exportadas diretamente para Parquet via `COPY ... TO ... (FORMAT PARQUET)`.

```bash
Rscript bin/datajud_parse.R
```

**Saída:**

| Tabela | Descrição |
|--------|-----------|
| `processos.parquet` | Dados cadastrais do processo (classe, órgão julgador, data de ajuizamento, grau, tribunal) |
| `assuntos.parquet` | Assuntos CNJ desaninhados (um registro por assunto × processo) |
| `movimentos.parquet` | Movimentos processuais com complementos tabelados desaninhados |

**Dependências:** `duckdb`, `DBI`, `glue`, `stringr`

---

### `datajud_load_n_clean_logger.R` — Carga, Limpeza e Enriquecimento

Carrega os três Parquet via DuckDB, executa pipeline de limpeza com tipagem explícita, parsing de timestamps heterogêneos (ISO 8601 com/sem `Z`, formato `yyyyMMddHHmmss`, `yyyyMMdd`) e enriquecimento geográfico via API IBGE (`/api/v1/localidades/municipios/{codigo}`), adicionando nome do município e mesorregião. Possui detecção de modo de execução: quando invocado via `source()` retorna os dataframes no ambiente chamador; quando via `Rscript` persiste RDS em `data/cleaned/`.

O logging é implementado com `logger` em duplo appender: console com cores ANSI e arquivo `.log` limpo em `logs/`.

```bash
Rscript bin/datajud_load_n_clean_logger.R
```

**Dependências:** `duckdb`, `tidyverse`, `dplyr`, `httr`, `purrr`, `jsonlite`, `lubridate`, `splines`, `segmented`, `stargazer`, `plotly`, `htmlwidgets`, `knitr`, `kableExtra`, `scales`, `logger`

---

## Execução Completa

```bash
# 1. Extração — todas as classes falimentares do TJPR
Rscript bin/datajud_obter_dados_classe_paralelo_windows.R tjpr 108 111 114 128 129

# 2. Parsing NDJSON → Parquet via DuckDB
Rscript bin/datajud_parse.R

# 3. Carga, limpeza e enriquecimento IBGE
Rscript bin/datajud_load_n_clean_logger.R
```

Para uso interativo no RStudio, carregue o ambiente limpo via:

```r
source("bin/datajud_load_n_clean_logger.R")
# → disponibiliza df_processos, df_movimentos, df_assuntos no ambiente global
```

---

## Autenticação API DataJud

A API DataJud utiliza autenticação via `ApiKey` no header HTTP. A chave pública do CNJ está hardcoded no script de extração. Caso o CNJ revogue ou atualize a chave, edite o vetor `header` em `datajud_obter_dados_classe_paralelo_windows.R`:

```r
header <- c(
  "Authorization" = "ApiKey <nova_chave>",
  "Content-Type"  = "application/json"
)
```

---

## Fonte de Dados

- **API DataJud** — CNJ (Conselho Nacional de Justiça)  
  Documentação: [https://datajud-wiki.cnj.jus.br](https://datajud-wiki.cnj.jus.br)
- **API IBGE** — Localidades  
  `https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{codigo}`
- **Tribunal:** TJPR — Tribunal de Justiça do Estado do Paraná
- **Período de referência:** 2015–2025

---

## Licença

Uso restrito. Dados públicos sujeitos aos termos de uso da API DataJud/CNJ.
