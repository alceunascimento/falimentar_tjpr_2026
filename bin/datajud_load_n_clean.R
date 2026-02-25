#!/usr/bin/env Rscript

# INFO -------------------------------------------------------------------------
# Autor: Alceu Eilert Nascimento (alceuenascimento@gmail.com)
# Instituição: Observatório do Poder Judiciário (OAB Paraná)
# Data: 2026-01-10
# Objetivo: Carregar, validar e limpar os parquet(s) obtidos do DataJud 
# Requisitos: os parquet de ingestão terem sido criados pelo `datajud_parse.R`
# Execução: `Rscript bin/datajud_load_n_clean.R`
# Notas: 
#   - é necessário um Rproj para o projeto 
#   - execute este script a partir do diretório raiz do projeto
#   - os parquet devem estar em `data/parsed/` e seguir a estrutura de nomes:
#      - parquet com os dados básicos dos processos: processos.parquet
#      - parquet com os movimentos dos processos: movimentos.parquet
#      - parquet com os assuntos dos processos: assuntos.parquet

## LOG AND ERROR CONTROL -------------------------------------------------------
start_time <- Sys.time()

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(logger)

DIR_LOG <- file.path(ROOT, "logs")
if (!dir.exists(DIR_LOG)) dir.create(DIR_LOG, recursive = TRUE)

log_file <- file.path(
  DIR_LOG,
  paste0("datajud_load_n_clean_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)
log_appender(appender_console,       index = 1)
log_layout(layout_glue_colors,       index = 1)   # colorido no console
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,              index = 2)   # limpo no arquivo

log_info("Pipeline iniciado — arquivo de log: {log_file}")

## INFO NO STDOUT --------------------------------------------------------------
cat("-----------------------------------------------------------------------------------\n")
cat("-------- DATAJUD: EXECUTANDO RSCRIPT DE CARREGAMENTO E LIMPEZA DOS PARQUET --------\n")
cat("-----------------------------------------------------------------------------------\n")
cat(sprintf("Início do script em %s", format(start_time, "%Y-%m-%d %H:%M:%S\n")))
cat("-----------------------------------------------------------------------------------\n")
rproj_files <- list.files(ROOT, pattern = "\\.Rproj$", full.names = TRUE)
if (length(rproj_files) == 0) {
  stop(
    "[ERRO] Nenhum .Rproj encontrado em getwd(). ",
    "[AVISO] Execute este script a partir do diretório raiz do projeto!"
  )
}
cat("Projeto detectado: ", basename(rproj_files[1]))
cat("-----------------------------------------------------------------------------------\n")
cat("   LOG DE EXECUÇÃO\n")
cat("   Arquivo:", log_file, "\n")
cat("-----------------------------------------------------------------------------------\n")
cat(
"
Autor: Alceu Eilert Nascimento (alceuenascimento@gmail.com)
Instituição: Observatório do Poder Judiciário (OAB Paraná)
Data: 2026-01-10

Objetivo:
  Carregar, validar e limpar os dados obtidos do DataJud.

Requisitos:
  - Os parquet de ingestão devem ter sido gerados por `datajud_parse.R`.

Execução:
  Rscript bin/datajud_load_n_clean.R

Notas operacionais:
  - O projeto deve conter um arquivo .Rproj na raiz.
  - Execute este script a partir do diretório raiz do projeto.
  - Os arquivos devem estar em: data/parsed/

Estrutura esperada:
  processos.parquet   -> dados básicos dos processos
  movimentos.parquet  -> movimentos processuais
  assuntos.parquet    -> assuntos dos processos
"
)

# SETUP ------------------------------------------------------------------------
cat("\n[INFO] SETUP ------------------------------------------------------------------\n")

agora_utc <- as.POSIXct(Sys.time(), tz = "UTC")

## pacotes ----
cat("[INFO] Carregando pacotes (via pacman)...\n")
cat("[AVISO] Se necessário, os pacotes ausentes serão instalados (pode levar um tempo)!\n")

pacman::p_load(
  duckdb,
  tidyverse,
  dplyr,
  httr,
  purrr,
  jsonlite,
  lubridate,
  splines,
  segmented,
  stargazer,
  plotly,
  htmlwidgets,
  knitr,
  kableExtra,
  scales
  
)
cat("[OK] Pacotes carregados!\n")

## funcoes ----
cat("[INFO] Carregando funções auxiliares...\n")


### funcao para verificar existencia dos parquet ----
check_parquet_parsed <- function(root = getwd()) {
  
  cat("[INFO] [CHECK] Verificando arquivos Parquet em data/parsed...\n")
  
  dir_parsed <- file.path(root, "data", "parsed")
  
  arquivos_esperados <- c(
    "processos.parquet",
    "movimentos.parquet",
    "assuntos.parquet"
  )
  
  caminhos <- file.path(dir_parsed, arquivos_esperados)
  existe <- file.exists(caminhos)
  
  resultado <- data.frame(
    arquivo = arquivos_esperados,
    caminho = caminhos,
    existe = existe,
    status = ifelse(existe, "OK", "ERRO"),
    stringsAsFactors = FALSE
  )
  
  print(resultado, row.names = FALSE)
  
  if (!all(existe)) {
    faltantes <- arquivos_esperados[!existe]
    
    cat("\n[ERRO] Arquivos obrigatórios não encontrados:\n")
    cat(paste0(" - ", faltantes, collapse = "\n"), "\n")
    
    stop("[FATAL] Pipeline interrompido: execute datajud_parse.R antes.")
  }
  
  cat("[OK] Todos os arquivos Parquet necessários estão disponíveis.\n\n")
  
  invisible(resultado)
}


### função para consultar a API do IBGE ----
obter_dados_municipio_ibge <- function(codigo) {
  
  url <- paste0(
    "https://servicodados.ibge.gov.br/api/v1/localidades/municipios/",
    codigo
  )
  
  resp <- try(GET(url), silent = TRUE)
  
  if (inherits(resp, "try-error") || status_code(resp) != 200) {
    return(tibble(
      orgao_julgador_municipio_ibge = codigo,
      municipio_nome = NA_character_,
      municipio_mesorregiao = NA_character_
    ))
  }
  
  conteudo <- content(resp, as = "text", encoding = "UTF-8")
  json <- fromJSON(conteudo, flatten = TRUE)
  
  tibble(
    orgao_julgador_municipio_ibge = codigo,
    municipio_nome = json$nome,
    municipio_mesorregiao = json$microrregiao.mesorregiao.nome
  )
}


### função para check dados de tempo ----
perfil_data <- function(x, n_exemplos = 5) {
  tibble(x = as.character(x)) |>
    mutate(
      x_trim = str_trim(x),
      perfil = case_when(
        is.na(x_trim) | x_trim == "" ~ "vazio/NA",
        
        # ISO 8601 com Z (ex: 2025-04-02T15:16:30.000Z ou sem milissegundo)
        str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$") ~ "iso8601_Z",
        
        # ISO 8601 sem Z (às vezes aparece)
        str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?$") ~ "iso8601_semZ",
        
        # Numérico: YYYYMMDDHHMMSS (14 dígitos) ex: 20250414151934
        str_detect(x_trim, "^\\d{14}$") ~ "num_yyyymmddhhmmss",
        
        # Numérico: YYYYMMDDHHMM (12 dígitos) ex: 20240717171140
        str_detect(x_trim, "^\\d{12}$") ~ "num_yyyymmddhhmm",
        
        # Numérico: YYYYMMDD (8 dígitos)
        str_detect(x_trim, "^\\d{8}$") ~ "num_yyyymmdd",
        
        TRUE ~ "outro"
      )
    ) |>
    count(perfil, sort = TRUE) |>
    left_join(
      tibble(x = as.character(x)) |>
        mutate(
          x_trim = str_trim(x),
          perfil = case_when(
            is.na(x_trim) | x_trim == "" ~ "vazio/NA",
            str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$") ~ "iso8601_Z",
            str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?$") ~ "iso8601_semZ",
            str_detect(x_trim, "^\\d{14}$") ~ "num_yyyymmddhhmmss",
            str_detect(x_trim, "^\\d{12}$") ~ "num_yyyymmddhhmm",
            str_detect(x_trim, "^\\d{8}$") ~ "num_yyyymmdd",
            TRUE ~ "outro"
          )
        ) |>
        group_by(perfil) |>
        summarise(exemplos = paste(head(na.omit(unique(x_trim)), n_exemplos), collapse = " | "), .groups = "drop"),
      by = "perfil"
    )
}

## funcao para corrigir data numérica ----
parse_data_ajuizamento_utc <- function(x) {
  x <- str_trim(as.character(x))
  
  out <- case_when(
    is.na(x) | x == "" ~ as.POSIXct(NA),
    
    # ISO 8601 com Z (com ou sem milissegundos)
    str_detect(x, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$") ~
      as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
    
    # Numérico 14 dígitos: YYYYMMDDHHMMSS
    str_detect(x, "^\\d{14}$") ~
      as.POSIXct(x, format = "%Y%m%d%H%M%S", tz = "UTC"),
    
    TRUE ~ as.POSIXct(NA)
  )
  
  out
}

## funcao para verificar NAs ----
check_na_df <- function(df, nome) {
  total_rows <- nrow(df)
  
  na_por_coluna <- colSums(is.na(df))
  
  tibble(
    dataframe = nome,
    n_linhas = total_rows,
    n_colunas = ncol(df),
    n_celulas_na = sum(na_por_coluna),
    pct_celulas_na = sum(na_por_coluna) / (total_rows * ncol(df)),
    n_colunas_com_na = sum(na_por_coluna > 0)
  )
}

## função de Quality Assurance (QA) (verificar NAs por dataframe com regras) ----
qa_sem_na <- function(df, nome) {
  
  linhas_com_na <- sum(!complete.cases(df))
  celulas_na <- sum(is.na(df))
  colunas_com_na <- sum(colSums(is.na(df)) > 0)
  
  tibble(
    dataframe = nome,
    linhas = nrow(df),
    colunas = ncol(df),
    linhas_com_na = linhas_com_na,
    colunas_com_na = colunas_com_na,
    celulas_na = celulas_na,
    status = ifelse(linhas_com_na == 0, "OK", "ERRO")
  )
}

qa_movimentos <- function(df) {
  
  colunas_obrigatorias <- c(
    "id",
    "processo",
    "movimento_codigo",
    "movimento_nome",
    "movimento_data_hora"
  )
  
  colunas_complemento <- c(
    "complemento_codigo",
    "complemento_nome",
    "complemento_valor",
    "complemento_descricao"
  )
  
  # garantir que só usamos colunas existentes (robustez)
  colunas_obrigatorias <- intersect(colunas_obrigatorias, names(df))
  colunas_complemento <- intersect(colunas_complemento, names(df))
  
  linhas_com_na_obrigatorio <- sum(rowSums(is.na(df[, colunas_obrigatorias, drop = FALSE])) > 0)
  celulas_na_obrigatorio <- sum(is.na(df[, colunas_obrigatorias, drop = FALSE]))
  
  linhas_sem_complemento <- if (length(colunas_complemento) > 0) {
    sum(rowSums(is.na(df[, colunas_complemento, drop = FALSE])) == length(colunas_complemento))
  } else {
    NA_integer_
  }
  
  celulas_na_complemento <- if (length(colunas_complemento) > 0) {
    sum(is.na(df[, colunas_complemento, drop = FALSE]))
  } else {
    NA_real_
  }
  
  status_obrigatorio <- ifelse(linhas_com_na_obrigatorio == 0, "OK", "ERRO")
  status_complemento <- "OK"  # por regra: NA em complemento é esperado
  
  mensagem <- if (status_obrigatorio == "OK") {
    paste0(
      "Obrigatórias OK; ",
      linhas_sem_complemento, " linhas sem complemento"
    )
  } else {
    paste0(
      "ERRO: ", linhas_com_na_obrigatorio, " linhas com NA em obrigatórias"
    )
  }
  
  tibble(
    dataframe = "df_movimentos",
    linhas = nrow(df),
    
    # Integridade (erro)
    linhas_com_na_obrigatorio = linhas_com_na_obrigatorio,
    celulas_na_obrigatorio = celulas_na_obrigatorio,
    status_obrigatorio = status_obrigatorio,
    
    # Complementos (esperado)
    linhas_sem_complemento = linhas_sem_complemento,
    celulas_na_complemento = celulas_na_complemento,
    status_complemento = status_complemento,
    
    # Log-friendly
    mensagem = mensagem
  )
}

## funcao para perguntar se salva os dataframes limpos em RDS ----
perguntar_salvar <- function() {
  
  if (!interactive()) {
    cat("[INFO] Sessão não interativa: não será solicitado input. Padrão = NÃO salvar.\n")
    return(FALSE)
  }
  
  resposta <- readline(
    prompt = "\n[ATENCAO] Deseja salvar os dataframes como RDS em data/cleaned? (s/n): "
  )
  
  tolower(trimws(resposta)) %in% c("s", "sim", "y", "yes")
}


cat("[OK] Funções auxiliares carregadas!\n")

# CONEXÃO E CONSULTA -----------------------------------------------------
cat("\n[INFO] LOAD DATA -------------------------------------------------------------\n")

## verifica se os parquet existem no data/parsed
check_parquet_parsed(ROOT)

## existindo os arquivos, segue o carregamento 
cat("[INFO] Conectando ao DuckDB e carregando os dados...\n")

con <- dbConnect(duckdb())

df_processos <- dbGetQuery(con,"
    SELECT 
    *
    FROM read_parquet('data/parsed/processos.parquet')
")

df_movimentos <- dbGetQuery(con,"
    SELECT 
    *
    FROM read_parquet('data/parsed/movimentos.parquet')
")

df_assuntos <- dbGetQuery(con,"
    SELECT 
    *
    FROM read_parquet('data/parsed/assuntos.parquet')
")

dbDisconnect(con, shutdown = TRUE)

cat(
  "[OK] Dados carregados. Tempo gasto: ", 
  round(
    difftime(
      Sys.time(), 
      start_time, 
      units = "secs"
    ), 
    2
  )
  , " segundos.\n"
)


# DATA CHECK -------------------------------------------------------------------
cat("\n[INFO] CHECK DATA ------------------------------------------------------------\n")

## check loaded data ----
cat("[INFO] [CHECK] Dataframes carregados:\n")
glimpse(df_processos)
glimpse(df_movimentos)
glimpse(df_assuntos)

## check time data ----
cat("\n[INFO] [CHECK] ------------------------------------------------------------------\n")
cat("[INFO] [CHECK] Verificando perfil de dados de data e hora nos dataframes...\n")
cat("[INFO] [CHECK] ------------------------------------------------------------------\n")

cat("\n[INFO] [CHECK] [df_processos] dados da variavel `data_ajuizamento`...\n")
print(perfil_data(df_processos$data_ajuizamento))
cat("----------------------------------------------------------------------------------\n")

cat("\n[INFO] [CHECK] [df_movimentos] dados da variavel `movimento_data_hora` (leva tempo)...\n")
print(perfil_data(df_movimentos$movimento_data_hora))
cat("----------------------------------------------------------------------------------\n")

## check missing data ----
cat("\n[INFO] [CHECK] Verificando dados faltantes (NA)...\n")

tabela_na <- bind_rows(
  check_na_df(df_processos, "df_processos"),
  check_na_df(df_movimentos, "df_movimentos"),
  check_na_df(df_assuntos, "df_assuntos")
)
print(tabela_na, n = Inf)
# alerta se houver problema relevante
if (any(tabela_na$n_celulas_na > 0)) {
  cat("[AVISO] [CHECK]: existem valores ausentes em um ou mais dataframes!\n")
} else {
  cat("[OK] [CHECK]: nenhum valor ausente identificado.\n")
}
cat("----------------------------------------------------------------------------------\n")

### executar QA -----
cat("\n[INFO] [CHECK] ------------------------------------------------------------------------------\n")
cat("[INFO] [CHECK] Executando Quality Assurance (QA) para verificar integridade dos dados...\n")
cat("[INFO] [CHECK] -----------------------------------------------------------------------------\n")

cat("\n[INFO] [CHECK] Verificando QA nos `df_processos` e `df_assuntos`...\n")
tabela_qa_critica <- bind_rows(
  qa_sem_na(df_processos, "df_processos"),
  qa_sem_na(df_assuntos,  "df_assuntos")
)
print(tabela_qa_critica, n = Inf)
if (any(tabela_qa_critica$status == "ERRO")) {
  stop("[CUIDADO] [CHECK]: encontrados `NAs` em dataframes críticos!!\n")
} else {
  cat("[OK] [CHECK]: dataframes críticos sem NAs.\n")
}
cat("---------------------------------------------------------------------------------------\n")

cat("\n[INFO] [CHECK] Verificando QA no `df_movimentos`...\n")
tabela_qa_mov <- qa_movimentos(df_movimentos)
print(tabela_qa_mov)
cat("[INFO] [CHECK] : ", tabela_qa_mov$mensagem,"\n")

if (tabela_qa_mov$status_obrigatorio == "ERRO") {
  stop("[CUIDADO] [CHECK]: `df_movimentos` tem `NAs` em campos obrigatórios!!\n")
} else {
  cat("[OK] [CHECK]: `df_movimentos` íntegro (`NAs` aceitos nas variaveis 'complementos_*')\n")
}
cat("----------------------------------------------------------------------------------------\n")

cat("\n[OK] [CHECK] Verificações de integridade e qualidade concluídas.\n")

# DATA CLEAN -------------------------------------------------------------------
cat("\n[INFO] CLEAN DATA ------------------------------------------------------------------------\n")

cat("\n------------------------------------------------------------------------------------------\n")
cat("[INFO] [CLEAN] Iniciando limpeza e análise exploratória...\n")
cat("------------------------------------------------------------------------------------------\n")

## ajuste no df_processos ----
cat("\n[INFO] [CLEAN] [df_processos] Ajustando tipos (as factor) e informações temporais...\n")
df_processos <- df_processos |>
  mutate(
    across(
      c(
        classe_codigo,
        classe_nome,
        orgao_julgador_codigo,
        orgao_julgador_nome,
        orgao_julgador_municipio_ibge,
        tribunal,
        grau
      ),
      as.factor
    )
  ) |> 
  mutate(
    data_ajuizamento_raw = data_ajuizamento,
    data_ajuizamento = parse_data_ajuizamento_utc(data_ajuizamento_raw),
    ano_ajuizamento = year(data_ajuizamento),
    mes_ajuizamento = month(data_ajuizamento)
  )

cat("[OK] [CLEAN] [df_processos] Tipos ajustados e variáveis de data criadas.\n")

#### verificando ajustes de datas na variavel `data_ajuizamento` ----
cat("\n[INFO] [CLEAN] [df_processos] [CHECK] Iniciado, acompanhe os resultados dos dados.\n")

cat("[INFO] [CLEAN] [df_processos] [CHECK] Verificando resultado dos ajustes dos dados na variavel `data_ajuizamento`...\n")

##### os dados na variavel `movimento_data_hora_raw` são numéricos (queremos FALSE)? ----
tipo_raw <- class(df_processos$data_ajuizamento_raw)
eh_numerico <- is.numeric(df_processos$data_ajuizamento_raw)

cat(
  "[INFO] [CLEAN] [df_processos] [CHECK] [1] Tipo de `data_ajuizamento_raw`: ",
  paste(tipo_raw, collapse = ", ")
)
if (eh_numerico) {
  warning(
    "[CUIDADO] `data_ajuizamento_raw` é numérico. ",
    "Há risco de perda de precisão em timestamps (ex.: YYYYMMDDHHMMSS)."
  )
} else {
  cat(
    " `data_ajuizamento_raw` não é numérico (esperado).\n"
  )
}

##### resumo do parse de `data_ajuizamento` ----
resumo_parse <- df_processos |>
  summarise(
    quantidade_processos  = n(),
    quantidade_parse_ok   = sum(!is.na(data_ajuizamento)),
    quantidade_parse_erro = sum(is.na(data_ajuizamento))
  )
cat(
  sprintf(
    "[INFO] [CLEAN] [df_processos] [CHECK] [2] Resultado do parse de datas: total = %d | ok = %d | erro = %d\n",
    resumo_parse$quantidade_processos,
    resumo_parse$quantidade_parse_ok,
    resumo_parse$quantidade_parse_erro
  )
)

##### verificando o intervalo de datas válidas na variavel `data_ajuizamento` ----
resumo_intervalos <- df_processos |>
  filter(!is.na(data_ajuizamento)) |>
  summarise(
    min_data = min(data_ajuizamento),
    max_data = max(data_ajuizamento)
  )
cat(
  sprintf(
    "[INFO] [CLEAN] [df_processos] [CHECK] [3] Intervalo de `data_ajuizamento`: primeira = %s | última = %s\n",
    resumo_intervalos$min_data,
    resumo_intervalos$max_data
  )
)

##### verificando processos sem dados de "hora" na variavel `data_ajuizamento` ----
cat("[INFO] [CLEAN] [df_processos] [CHECK] [4] Processos sem 'hora' na variavel `data_ajuizamento`:\n")
tabela_meianoite <- df_processos |>
  filter(!is.na(data_ajuizamento)) |>
  mutate(
    eh_meianoite = format(data_ajuizamento, "%H%M%S") == "000000",
    raw_meianoite = str_detect(data_ajuizamento_raw, "T00:00:00(\\.000)?Z$")
  ) |>
  count(eh_meianoite, raw_meianoite) |>
  mutate(
    percentual = n / sum(n),
    significado = case_when(
      !eh_meianoite & !raw_meianoite ~ "Data com hora na origem (datetime completo)",
      eh_meianoite & raw_meianoite ~ "Data sem hora na origem (formato raw ISO com 00:00:00)",
      eh_meianoite & !raw_meianoite ~ "Data sem hora na origem (formato raw YYYYMMDD000000)",
      TRUE ~ "Outro caso"
    ),
    n = comma(n),
    percentual = percent(percentual, accuracy = 0.1)
  )
print(tabela_meianoite)

cat("\n[INFO] [CLEAN] [df_processos] [CHECK] finalizado, verifique os resultados dos dados.\n")


### busca informações do municipio na API do IBGE ----
cat("\n[INFO] [CLEAN] [df_processos] [ADD DATA] ----------------\n")
cat("[INFO] [CLEAN] [df_processos] [ADD DATA] Buscando informações de município na API do IBGE...\n")

#### 1. garante que o código IBGE é numérico ----
df_processos <- df_processos |>
  mutate(
    orgao_julgador_municipio_ibge = as.integer(as.character(orgao_julgador_municipio_ibge))
  )

#### 2. extrai códigos únicos (evita chamadas repetidas) ----
codigos_ibge <- df_processos |>
  distinct(orgao_julgador_municipio_ibge) |>
  filter(!is.na(orgao_julgador_municipio_ibge)) |>
  pull(orgao_julgador_municipio_ibge)

#### 3. Consulta todos os municípios ----
tabela_municipios <- map_dfr(
  codigos_ibge, 
  obter_dados_municipio_ibge
)
cat(
  "[OK] [CLEAN] [df_processos] [ADD DATA] Consulta na API do IBGE concluída. Tempo gasto: ", 
  round(
    difftime(
      Sys.time(), 
      start_time, 
      units = "secs"
    ), 
    2
  ),
  " segundos.\n"
)

cat("[INFO] [CLEAN] [df_processos] [ADD DATA] Municipios presentes nos dados:\n")
print(unique(tabela_municipios$municipio_nome))

#### 4. Faz join com o dataframe df_processos ----
df_processos <- df_processos |>
  left_join(
    tabela_municipios, 
    by = "orgao_julgador_municipio_ibge"
  )
cat("-----------------------------------------------------------------------------------\n")

cat("\n[OK] [CLEAN] [df_processos] Tipos ajustados, informações de município adicionadas e dados verificados, explorando o dataframe após ajuste:\n")
glimpse(df_processos)

## ajuste no df_movimentos ----
cat("-----------------------------------------------------------------------------------\n")
cat("\n[INFO] [CLEAN] [df_movimentos] Ajustando tipos (as factor)...\n")
df_movimentos <- df_movimentos |>
  mutate(
    across(
      c(
        movimento_codigo,
        movimento_nome,
        complemento_codigo,
        complemento_nome,
        complemento_valor,
        complemento_descricao
      ),
      as.factor
    )
  ) |> 
  mutate(
    movimento_data_hora_raw = movimento_data_hora,
    movimento_data_hora = parse_data_ajuizamento_utc(movimento_data_hora_raw),
    movimento_mes = floor_date(movimento_data_hora, "month"),
    movimento_ano = year(movimento_data_hora)
  )

cat("[OK] [CLEAN] [df_movimentos] Tipos ajustados e variáveis de data criadas.\n")

#### verificando ajustes de datas na variavel `movimento_data_hora` ----
cat("\n[INFO] [CLEAN] [df_movimentos] [CHECK] Iniciado, acompanhe os resultados dos dados.\n")

cat("[INFO] [CLEAN] [df_movimentos] [CHECK] Verificando dados na variavel `movimento_data_hora`...\n")


##### os dados na variavel `movimento_data_hora_raw` são numéricos (queremos FALSE)? ----
tipo_raw <- class(df_movimentos$movimento_data_hora)
eh_numerico <- is.numeric(df_movimentos$movimento_data_hora)

cat(
  "[INFO] [CLEAN] [df_movimentos] [CHECK] [1] Tipo de `movimento_data_hora_raw`: ",
  paste(tipo_raw, collapse = ", ")
)

if (eh_numerico) {
  warning(
    "[CUIDADO] `movimento_data_hora_raw` é numérico. ",
    "Há risco de perda de precisão em timestamps (ex.: YYYYMMDDHHMMSS)."
  )
} else {
  cat(
    "`movimento_data_hora_raw` não é numérico (esperado).\n"
  )
}


##### resumo do parse de `movimento_data_hora` ----
resumo_parse <- df_movimentos |>
  summarise(
    quantidade_movimentos  = n(),
    quantidade_parse_ok   = sum(!is.na(movimento_data_hora)),
    quantidade_parse_erro = sum(is.na(movimento_data_hora))
  )
cat(
  sprintf(
    "[INFO] [CLEAN] [df_movimentos] [CHECK] [2] Resultado do parse de datas: total = %d | ok = %d | erro = %d\n",
    resumo_parse$quantidade_movimentos,
    resumo_parse$quantidade_parse_ok,
    resumo_parse$quantidade_parse_erro
  )
)

##### verificando o intervalo de datas válidas na variavel `movimento_data_hora` ----
resumo_intervalos <- df_movimentos |>
  filter(!is.na(movimento_data_hora)) |>
  summarise(
    min_data = min(movimento_data_hora),
    max_data = max(movimento_data_hora)
  )
cat(
  sprintf(
    "[INFO] [CLEAN] [df_movimentos] [CHECK] [3] Intervalo de `movimento_data_hora`: primeira = %s | última = %s\n",
    resumo_intervalos$min_data,
    resumo_intervalos$max_data
  )
)

##### verificando processos sem dados de "hora" na variavel `movimento_data_hora` ----
cat("[INFO] [CLEAN] [df_movimentos] [CHECK] [4] Processos sem 'hora' nas observações da variavel `movimento_data_hora`:\n")
tabela_meianoite <- df_movimentos |>
  filter(!is.na(movimento_data_hora)) |>
  mutate(
    eh_meianoite = format(movimento_data_hora, "%H%M%S") == "000000",
    raw_meianoite = str_detect(movimento_data_hora_raw, "T00:00:00(\\.000)?Z$")
  ) |>
  count(eh_meianoite, raw_meianoite) |>
  mutate(
    percentual = n / sum(n),
    significado = case_when(
      !eh_meianoite & !raw_meianoite ~ "Data com hora na origem (datetime completo)",
      eh_meianoite & raw_meianoite ~ "Data sem hora na origem (formato raw ISO com 00:00:00)",
      eh_meianoite & !raw_meianoite ~ "Data sem hora na origem (formato raw YYYYMMDD000000)",
      TRUE ~ "Outro caso"
    ),
    n = comma(n),
    percentual = percent(
      percentual, 
      accuracy = 0.1
    )
  )
print(tabela_meianoite)

cat("[INFO] [CLEAN] [df_movimentos] [CHECK] Finalizado, verifique os resultados dos dados.\n")


cat("\n[OK] [CLEAN] [df_movimentos] Tipos ajustados e verificados, explorando o dataframe de após ajuste:\n")
glimpse(df_movimentos)


## ajuste no df_assuntos ----
cat("-----------------------------------------------------------------------------------\n")
cat("\n[INFO] [CLEAN] [df_assuntos] Ajustando tipos (as factor)...\n")
df_assuntos <- df_assuntos |>
  mutate(
    across(
      c(
        assunto_codigo,
        assunto_nome
      ),
      as.factor
    )
  )
cat("[OK] [CLEAN] [df_assuntos] Tipos ajustados explorando o dataframe de após ajuste:\n")
glimpse(df_assuntos)
cat("-----------------------------------------------------------------------------------\n")

# EXPORTANDO OS DATAFRAMES LIMPOS (opcional) -----------------------------------
cat("\n[INFO] [OUTPUT] (opcional) -----------------------------------------------------------------\n")

## chamando a função de salvar -----
if (perguntar_salvar()) {
  cat("\n[INFO] Salvando arquivos como RDS em data/cleaned ...\n")

  # diretório raiz (assumindo execução a partir da raiz do projeto)
  DIR_CLEAN <- file.path(ROOT, "data", "cleaned")
  
  # cria diretório se não existir
  if (!dir.exists(DIR_CLEAN)) {
    dir.create(DIR_CLEAN, recursive = TRUE)
    cat("[INFO] Diretório criado: ", DIR_CLEAN, "\n", sep = "")
  }
  
  # caminhos dos arquivos
  arq_processos  <- file.path(DIR_CLEAN, "df_processos.rds")
  arq_movimentos <- file.path(DIR_CLEAN, "df_movimentos.rds")
  arq_assuntos   <- file.path(DIR_CLEAN, "df_assuntos.rds")
  
  saveRDS(df_processos, arq_processos)
  saveRDS(df_movimentos, arq_movimentos)
  saveRDS(df_assuntos, arq_assuntos)
  
  cat("[OK] Arquivos salvos:\n")
  cat("     df_processos  -> ", arq_processos,  "\n", sep = "")
  cat("     df_movimentos -> ", arq_movimentos, "\n", sep = "")
  cat("     df_assuntos   -> ", arq_assuntos,   "\n", sep = "")
  
} else {
  cat("\n[INFO] Salvamento ignorado (dados disponíveis apenas no ambiente da sessão).\n")
}


# FINALIZACAO --------------------------------------------------------------------------
rm(
  con,
  resumo_intervalos,
  resumo_parse,
  tabela_meianoite,
  tabela_municipios,
  tabela_na,
  tabela_qa_critica,
  tabela_qa_mov,
  agora_utc,
  codigos_ibge,
  eh_numerico,
  ROOT,
  DIR_CLEAN,
  DIR_LOG,
  log_file,
  rproj_files,
  arq_processos,
  arq_movimentos,
  arq_assuntos,
  tipo_raw,
  check_na_df,
  obter_dados_municipio_ibge,
  parse_data_ajuizamento_utc,
  perfil_data,
  qa_movimentos,
  qa_sem_na,
  perguntar_salvar,
  check_parquet_parsed
)


cat("\n--------------------------------------------------------------------------------------\n")
cat("[INFO] FIM DA EXECUÇÃO DO SCRIPT `Rscript bin/datajud_load_n_clean.R` ----------------\n")
cat("[OK] Dataframes carregados, limpos e prontos para análise. \n  [INFO] Tempo gasto: ", 
        round(difftime(Sys.time(), start_time, units = "secs"), 2), 
        " segundos.\n")

end_time <- Sys.time()
cat(sprintf("[INFO] Fim do script em %s", format(end_time, "%Y-%m-%d %H:%M:%S")))
cat("\n--------------------------------------------------------------------------------------\n")
log_info("Pipeline encerrado com sucesso")

rm(end_time, start_time)

#eof----------------------------------------------------------------------------