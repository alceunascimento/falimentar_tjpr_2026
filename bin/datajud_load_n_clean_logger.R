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
agora_utc <- as.POSIXct(Sys.time(), tz = "UTC")

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(logger)

DIR_LOG <- file.path(ROOT, "logs")
if (!dir.exists(DIR_LOG)) dir.create(DIR_LOG, recursive = TRUE)

log_file <- file.path(
  DIR_LOG,
  paste0("datajud_load_n_clean_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

# appender_tee com layouts distintos:
#   index = 1 → console com cores ANSI
#   index = 2 → arquivo limpo (sem escape codes)
log_threshold(DEBUG)
log_appender(appender_console,        index = 1)
log_layout(layout_glue_colors,        index = 1)
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,               index = 2)

log_info("Pipeline iniciado")

## BANNER INFORMATIVO ----------------------------------------------------------
cat("-----------------------------------------------------------------------------------\n")
cat("-------- DATAJUD: EXECUTANDO RSCRIPT DE CARREGAMENTO E LIMPEZA DOS PARQUET --------\n")
cat("-----------------------------------------------------------------------------------\n")
cat(sprintf("Início do script em %s\n", format(start_time, "%Y-%m-%d %H:%M:%S")))
cat("-----------------------------------------------------------------------------------\n")

rproj_files <- list.files(ROOT, pattern = "\\.Rproj$", full.names = TRUE)
if (length(rproj_files) == 0) {
  log_error("SETUP | Nenhum .Rproj encontrado em: {ROOT}")
  stop("Nenhum .Rproj encontrado em getwd(). Execute a partir da raiz do projeto.")
}

cat("Projeto detectado: ", basename(rproj_files[1]), "\n")
cat("Log de execução  : ", log_file, "\n")
cat("-----------------------------------------------------------------------------------\n")
cat("
INFORMAÇÕES
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
log_info("SETUP | Iniciando configuração")

## pacotes ----
log_info("SETUP | Carregando pacotes via pacman")
log_warn("SETUP | Pacotes ausentes serão instalados automaticamente (pode demorar)")
log_warn("SETUP | Se tiver que instarlar o duckdb vai demorar bastante, aguarde com paciência")

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
log_info("SETUP | Pacotes carregados")

## funcoes ----
log_info("SETUP | Carregando funções auxiliares")

### funcao para verificar existencia dos parquet ----
check_parquet_parsed <- function(root = getwd()) {
  
  log_debug("CHECK | Verificando arquivos Parquet em data/parsed")
  
  dir_parsed <- file.path(root, "data", "parsed")
  
  arquivos_esperados <- c(
    "processos.parquet",
    "movimentos.parquet",
    "assuntos.parquet"
  )
  
  caminhos <- file.path(dir_parsed, arquivos_esperados)
  existe   <- file.exists(caminhos)
  
  resultado <- data.frame(
    arquivo = arquivos_esperados,
    caminho = caminhos,
    existe  = existe,
    status  = ifelse(existe, "OK", "ERRO"),
    stringsAsFactors = FALSE
  )
  
  log_debug("CHECK | Resultado da verificação de parquet:\n{paste(capture.output(print(resultado, row.names = FALSE)), collapse = '\n')}")
  
  if (!all(existe)) {
    faltantes <- arquivos_esperados[!existe]
    log_error("CHECK | Arquivos obrigatórios não encontrados: {paste(faltantes, collapse = ', ')}")
    stop("Pipeline interrompido: execute datajud_parse.R antes.")
  }
  
  log_info("CHECK | Todos os arquivos Parquet estão disponíveis")
  invisible(resultado)
}

### função para consultar a API do IBGE ----
obter_dados_municipio_ibge <- function(codigo) {
  
  url  <- paste0("https://servicodados.ibge.gov.br/api/v1/localidades/municipios/", codigo)
  resp <- try(GET(url), silent = TRUE)
  
  if (inherits(resp, "try-error") || status_code(resp) != 200) {
    return(tibble(
      orgao_julgador_municipio_ibge = codigo,
      municipio_nome                = NA_character_,
      municipio_mesorregiao         = NA_character_
    ))
  }
  
  conteudo <- content(resp, as = "text", encoding = "UTF-8")
  json     <- fromJSON(conteudo, flatten = TRUE)
  
  tibble(
    orgao_julgador_municipio_ibge = codigo,
    municipio_nome                = json$nome,
    municipio_mesorregiao         = json$microrregiao.mesorregiao.nome
  )
}

### função para check dados de tempo ----
perfil_data <- function(x, n_exemplos = 5) {
  tibble(x = as.character(x)) |>
    mutate(
      x_trim = str_trim(x),
      perfil  = case_when(
        is.na(x_trim) | x_trim == ""                                                        ~ "vazio/NA",
        str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$")      ~ "iso8601_Z",
        str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?$")       ~ "iso8601_semZ",
        str_detect(x_trim, "^\\d{14}$")                                                     ~ "num_yyyymmddhhmmss",
        str_detect(x_trim, "^\\d{12}$")                                                     ~ "num_yyyymmddhhmm",
        str_detect(x_trim, "^\\d{8}$")                                                      ~ "num_yyyymmdd",
        TRUE                                                                                ~ "outro"
      )
    ) |>
    count(perfil, sort = TRUE) |>
    left_join(
      tibble(x = as.character(x)) |>
        mutate(
          x_trim = str_trim(x),
          perfil  = case_when(
            is.na(x_trim) | x_trim == ""                                                        ~ "vazio/NA",
            str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$")      ~ "iso8601_Z",
            str_detect(x_trim, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?$")       ~ "iso8601_semZ",
            str_detect(x_trim, "^\\d{14}$")                                                     ~ "num_yyyymmddhhmmss",
            str_detect(x_trim, "^\\d{12}$")                                                     ~ "num_yyyymmddhhmm",
            str_detect(x_trim, "^\\d{8}$")                                                      ~ "num_yyyymmdd",
            TRUE                                                                                ~ "outro"
          )
        ) |>
        group_by(perfil) |>
        summarise(
          exemplos = paste(head(na.omit(unique(x_trim)), n_exemplos), collapse = " | "),
          .groups  = "drop"
        ),
      by = "perfil"
    )
}

### funcao para corrigir data numerica ----
parse_data_ajuizamento_utc <- function(x) {
  x <- str_trim(as.character(x))
  case_when(
    is.na(x) | x == ""                                                            ~ as.POSIXct(NA),
    str_detect(x, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$")     ~
      as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"),
    str_detect(x, "^\\d{14}$")                                                    ~
      as.POSIXct(x, format = "%Y%m%d%H%M%S", tz = "UTC"),
    TRUE                                                                          ~ as.POSIXct(NA)
  )
}

### funcao para verificar NAs ----
check_na_df <- function(df, nome) {
  na_por_coluna <- colSums(is.na(df))
  tibble(
    dataframe       = nome,
    n_linhas        = nrow(df),
    n_colunas       = ncol(df),
    n_celulas_na    = sum(na_por_coluna),
    pct_celulas_na  = sum(na_por_coluna) / (nrow(df) * ncol(df)),
    n_colunas_com_na = sum(na_por_coluna > 0)
  )
}

### função de Quality Assurance (QA) ----
qa_sem_na <- function(df, nome) {
  linhas_com_na <- sum(!complete.cases(df))
  tibble(
    dataframe     = nome,
    linhas        = nrow(df),
    colunas       = ncol(df),
    linhas_com_na = linhas_com_na,
    colunas_com_na = sum(colSums(is.na(df)) > 0),
    celulas_na    = sum(is.na(df)),
    status        = ifelse(linhas_com_na == 0, "OK", "ERRO")
  )
}

qa_movimentos <- function(df) {
  
  colunas_obrigatorias <- intersect(
    c("id", "processo", "movimento_codigo", "movimento_nome", "movimento_data_hora"),
    names(df)
  )
  colunas_complemento <- intersect(
    c("complemento_codigo", "complemento_nome", "complemento_valor", "complemento_descricao"),
    names(df)
  )
  
  linhas_com_na_obrigatorio <- sum(rowSums(is.na(df[, colunas_obrigatorias, drop = FALSE])) > 0)
  celulas_na_obrigatorio    <- sum(is.na(df[, colunas_obrigatorias, drop = FALSE]))
  
  linhas_sem_complemento <- if (length(colunas_complemento) > 0)
    sum(rowSums(is.na(df[, colunas_complemento, drop = FALSE])) == length(colunas_complemento))
  else NA_integer_
  
  celulas_na_complemento <- if (length(colunas_complemento) > 0)
    sum(is.na(df[, colunas_complemento, drop = FALSE]))
  else NA_real_
  
  status_obrigatorio <- ifelse(linhas_com_na_obrigatorio == 0, "OK", "ERRO")
  
  mensagem <- if (status_obrigatorio == "OK")
    paste0("Obrigatórias OK; ", linhas_sem_complemento, " linhas sem complemento")
  else
    paste0("ERRO: ", linhas_com_na_obrigatorio, " linhas com NA em obrigatórias")
  
  tibble(
    dataframe                 = "df_movimentos",
    linhas                    = nrow(df),
    linhas_com_na_obrigatorio = linhas_com_na_obrigatorio,
    celulas_na_obrigatorio    = celulas_na_obrigatorio,
    status_obrigatorio        = status_obrigatorio,
    linhas_sem_complemento    = linhas_sem_complemento,
    celulas_na_complemento    = celulas_na_complemento,
    status_complemento        = "OK",
    mensagem                  = mensagem
  )
}

log_info("SETUP | Funções auxiliares carregadas")

# CONEXÃO E CONSULTA -----------------------------------------------------------
log_info("LOAD | Iniciando carregamento dos dados")

check_parquet_parsed(ROOT)

log_info("LOAD | Conectando ao DuckDB e lendo parquet")

con <- dbConnect(duckdb())

df_processos  <- dbGetQuery(con, "SELECT * FROM read_parquet('data/parsed/processos.parquet')")
df_movimentos <- dbGetQuery(con, "SELECT * FROM read_parquet('data/parsed/movimentos.parquet')")
df_assuntos   <- dbGetQuery(con, "SELECT * FROM read_parquet('data/parsed/assuntos.parquet')")

dbDisconnect(con, shutdown = TRUE)

log_info("LOAD | Dados carregados | tempo gasto: {round(difftime(Sys.time(), start_time, units = 'secs'), 1)}s")

# DATA CHECK -------------------------------------------------------------------
log_info("CHECK | Iniciando verificação de integridade")

## check loaded data ----
log_debug("CHECK | glimpse df_processos:\n{paste(capture.output(glimpse(df_processos)),  collapse = '\n')}")
log_debug("CHECK | glimpse df_movimentos:\n{paste(capture.output(glimpse(df_movimentos)), collapse = '\n')}")
log_debug("CHECK | glimpse df_assuntos:\n{paste(capture.output(glimpse(df_assuntos)),   collapse = '\n')}")

## check time data ----
log_info("CHECK | Verificando perfil de dados temporais")
log_debug("CHECK | perfil data_ajuizamento (df_processos):\n{paste(capture.output(print(perfil_data(df_processos$data_ajuizamento))),  collapse = '\n')}")
log_debug("CHECK | perfil movimento_data_hora (df_movimentos):\n{paste(capture.output(print(perfil_data(df_movimentos$movimento_data_hora))), collapse = '\n')}")

## check missing data ----
log_info("CHECK | Verificando dados faltantes (NA)")

tabela_na <- bind_rows(
  check_na_df(df_processos,  "df_processos"),
  check_na_df(df_movimentos, "df_movimentos"),
  check_na_df(df_assuntos,   "df_assuntos")
)

log_debug("CHECK | Tabela de NAs:\n{paste(capture.output(print(tabela_na, n = Inf)), collapse = '\n')}")

if (any(tabela_na$n_celulas_na > 0)) {
  log_warn("CHECK | NAs presentes | células ausentes: {sum(tabela_na$n_celulas_na)}")
} else {
  log_info("CHECK | Nenhum NA identificado nos dataframes")
}

## QA crítico (df_processos e df_assuntos) ----
log_info("CHECK | Executando QA em df_processos e df_assuntos")

tabela_qa_critica <- bind_rows(
  qa_sem_na(df_processos, "df_processos"),
  qa_sem_na(df_assuntos,  "df_assuntos")
)

log_debug("CHECK | Tabela QA crítica:\n{paste(capture.output(print(tabela_qa_critica, n = Inf)), collapse = '\n')}")

if (any(tabela_qa_critica$status == "ERRO")) {
  log_error("CHECK | QA FALHOU: NAs em dataframes críticos — abortando pipeline")
  stop("NAs encontrados em dataframes críticos.")
} else {
  log_info("CHECK | QA OK: df_processos e df_assuntos sem NAs")
}

## QA df_movimentos ----
log_info("CHECK | Executando QA em df_movimentos")

tabela_qa_mov <- qa_movimentos(df_movimentos)

log_debug("CHECK | Tabela QA movimentos:\n{paste(capture.output(print(tabela_qa_mov)), collapse = '\n')}")

log_info("CHECK | df_movimentos | {tabela_qa_mov$mensagem}")

if (tabela_qa_mov$status_obrigatorio == "ERRO") {
  log_error("CHECK | QA FALHOU: df_movimentos tem NAs em campos obrigatórios — abortando pipeline")
  stop("NAs em campos obrigatórios de df_movimentos.")
} else {
  log_info("CHECK | QA OK: df_movimentos íntegro (NAs aceitos em complemento_*)")
}

log_info("CHECK | Verificações de integridade concluídas")

# DATA CLEAN -------------------------------------------------------------------
log_info("CLEAN | Iniciando limpeza dos dataframes")

## ajuste no df_processos ----
log_info("CLEAN | df_processos | Ajustando tipos e variáveis temporais")

df_processos <- df_processos |>
  mutate(
    across(
      c(classe_codigo, classe_nome, orgao_julgador_codigo, orgao_julgador_nome,
        orgao_julgador_municipio_ibge, tribunal, grau),
      as.factor
    )
  ) |>
  mutate(
    data_ajuizamento_raw = data_ajuizamento,
    data_ajuizamento     = parse_data_ajuizamento_utc(data_ajuizamento_raw),
    ano_ajuizamento      = year(data_ajuizamento),
    mes_ajuizamento      = floor_date(data_ajuizamento, "month")
  )

log_info("CLEAN | df_processos | Tipos ajustados e variáveis de data criadas")

### verificando ajustes de datas na variavel `data_ajuizamento` ----
log_debug("CLEAN | df_processos | CHECK [1] tipo de data_ajuizamento_raw: {paste(class(df_processos$data_ajuizamento_raw), collapse = ', ')}")

if (is.numeric(df_processos$data_ajuizamento_raw)) {
  log_warn("CLEAN | df_processos | data_ajuizamento_raw é numérico — risco de perda de precisão em timestamps")
} else {
  log_debug("CLEAN | df_processos | data_ajuizamento_raw não é numérico (esperado)")
}

resumo_parse <- df_processos |>
  summarise(
    quantidade_processos  = n(),
    quantidade_parse_ok   = sum(!is.na(data_ajuizamento)),
    quantidade_parse_erro = sum(is.na(data_ajuizamento))
  )

log_info("CLEAN | df_processos | CHECK [2] parse datas | total: {resumo_parse$quantidade_processos} | ok: {resumo_parse$quantidade_parse_ok} | erro: {resumo_parse$quantidade_parse_erro}")

resumo_intervalos <- df_processos |>
  filter(!is.na(data_ajuizamento)) |>
  summarise(min_data = min(data_ajuizamento), max_data = max(data_ajuizamento))

log_info("CLEAN | df_processos | CHECK [3] intervalo data_ajuizamento | min: {resumo_intervalos$min_data} | max: {resumo_intervalos$max_data}")

tabela_meianoite <- df_processos |>
  filter(!is.na(data_ajuizamento)) |>
  mutate(
    eh_meianoite  = format(data_ajuizamento, "%H%M%S") == "000000",
    raw_meianoite = str_detect(data_ajuizamento_raw, "T00:00:00(\\.000)?Z$")
  ) |>
  count(eh_meianoite, raw_meianoite) |>
  mutate(
    percentual = n / sum(n),
    significado = case_when(
      !eh_meianoite & !raw_meianoite ~ "Data com hora na origem (datetime completo)",
      eh_meianoite  & raw_meianoite  ~ "Data sem hora na origem (formato raw ISO com 00:00:00)",
      eh_meianoite  & !raw_meianoite ~ "Data sem hora na origem (formato raw YYYYMMDD000000)",
      TRUE                           ~ "Outro caso"
    ),
    n          = comma(n),
    percentual = percent(percentual, accuracy = 0.1)
  )

log_debug("CLEAN | df_processos | CHECK [4] distribuição hora zero:\n{paste(capture.output(print(tabela_meianoite)), collapse = '\n')}")
log_info("CLEAN | df_processos | CHECK de datas concluído")

### busca informações do municipio na API do IBGE ----
log_info("CLEAN | df_processos | ADD DATA | Consultando API IBGE para municípios")

df_processos <- df_processos |>
  mutate(orgao_julgador_municipio_ibge = as.integer(as.character(orgao_julgador_municipio_ibge)))

codigos_ibge <- df_processos |>
  distinct(orgao_julgador_municipio_ibge) |>
  filter(!is.na(orgao_julgador_municipio_ibge)) |>
  pull(orgao_julgador_municipio_ibge)

tabela_municipios <- map_dfr(codigos_ibge, obter_dados_municipio_ibge)

log_info("CLEAN | df_processos | ADD DATA | API IBGE concluída | {nrow(tabela_municipios)} municípios | elapsed: {round(difftime(Sys.time(), start_time, units = 'secs'), 1)}s")
log_debug("CLEAN | df_processos | ADD DATA | municípios: {paste(unique(tabela_municipios$municipio_nome), collapse = ', ')}")

df_processos <- df_processos |>
  left_join(tabela_municipios, by = "orgao_julgador_municipio_ibge")

log_info("CLEAN | df_processos | Join IBGE concluído")
log_debug("CLEAN | df_processos | glimpse pós-ajuste:\n{paste(capture.output(glimpse(df_processos)), collapse = '\n')}")

## ajuste no df_movimentos ----
log_info("CLEAN | df_movimentos | Ajustando tipos e variáveis temporais")

df_movimentos <- df_movimentos |>
  mutate(
    across(
      c(movimento_codigo, movimento_nome, complemento_codigo,
        complemento_nome, complemento_valor, complemento_descricao),
      as.factor
    )
  ) |>
  mutate(
    movimento_data_hora_raw = movimento_data_hora,
    movimento_data_hora     = parse_data_ajuizamento_utc(movimento_data_hora_raw),
    movimento_mes           = floor_date(movimento_data_hora, "month"),
    movimento_ano           = year(movimento_data_hora)
  )

log_info("CLEAN | df_movimentos | Tipos ajustados e variáveis de data criadas")

### verificando ajustes de datas na variavel `movimento_data_hora` ----
log_debug("CLEAN | df_movimentos | CHECK [1] tipo de movimento_data_hora: {paste(class(df_movimentos$movimento_data_hora), collapse = ', ')}")

# [CORRIGIDO] warning() nativo → log_warn()
if (is.numeric(df_movimentos$movimento_data_hora)) {
  log_warn("CLEAN | df_movimentos | movimento_data_hora é numérico — risco de perda de precisão em timestamps")
} else {
  log_debug("CLEAN | df_movimentos | movimento_data_hora não é numérico (esperado)")
}

resumo_parse <- df_movimentos |>
  summarise(
    quantidade_movimentos = n(),
    quantidade_parse_ok   = sum(!is.na(movimento_data_hora)),
    quantidade_parse_erro = sum(is.na(movimento_data_hora))
  )

log_info("CLEAN | df_movimentos | CHECK [2] parse datas | total: {resumo_parse$quantidade_movimentos} | ok: {resumo_parse$quantidade_parse_ok} | erro: {resumo_parse$quantidade_parse_erro}")

resumo_intervalos <- df_movimentos |>
  filter(!is.na(movimento_data_hora)) |>
  summarise(min_data = min(movimento_data_hora), max_data = max(movimento_data_hora))

log_info("CLEAN | df_movimentos | CHECK [3] intervalo movimento_data_hora | min: {resumo_intervalos$min_data} | max: {resumo_intervalos$max_data}")

tabela_meianoite <- df_movimentos |>
  filter(!is.na(movimento_data_hora)) |>
  mutate(
    eh_meianoite  = format(movimento_data_hora, "%H%M%S") == "000000",
    raw_meianoite = str_detect(movimento_data_hora_raw, "T00:00:00(\\.000)?Z$")
  ) |>
  count(eh_meianoite, raw_meianoite) |>
  mutate(
    percentual  = n / sum(n),
    significado = case_when(
      !eh_meianoite & !raw_meianoite ~ "Data com hora na origem (datetime completo)",
      eh_meianoite  & raw_meianoite  ~ "Data sem hora na origem (formato raw ISO com 00:00:00)",
      eh_meianoite  & !raw_meianoite ~ "Data sem hora na origem (formato raw YYYYMMDD000000)",
      TRUE                           ~ "Outro caso"
    ),
    n          = comma(n),
    percentual = percent(percentual, accuracy = 0.1)
  )

log_debug("CLEAN | df_movimentos | CHECK [4] distribuição hora zero:\n{paste(capture.output(print(tabela_meianoite)), collapse = '\n')}")
log_info("CLEAN | df_movimentos | CHECK de datas concluído")
log_debug("CLEAN | df_movimentos | glimpse pós-ajuste:\n{paste(capture.output(glimpse(df_movimentos)), collapse = '\n')}")

## ajuste no df_assuntos ----
log_info("CLEAN | df_assuntos | Ajustando tipos")

df_assuntos <- df_assuntos |>
  mutate(across(c(assunto_codigo, assunto_nome), as.factor))

log_info("CLEAN | df_assuntos | Tipos ajustados")
log_debug("CLEAN | df_assuntos | glimpse pós-ajuste:\n{paste(capture.output(glimpse(df_assuntos)), collapse = '\n')}")

log_info("CLEAN | Limpeza dos dataframes concluída")

# EXPORTANDO OS DATAFRAMES LIMPOS (automatico se via terminal) -----------------
via_source <- sys.nframe() > 1

if (via_source) {
  log_info("OUTPUT | Executado via source() — dataframes disponíveis no ambiente chamador | RDS não gerado")
} else {
  log_info("OUTPUT | Executado via Rscript — salvando RDS em data/cleaned")
  
  DIR_CLEAN <- file.path(ROOT, "data", "cleaned")
  if (!dir.exists(DIR_CLEAN)) {
    dir.create(DIR_CLEAN, recursive = TRUE)
    log_info("OUTPUT | Diretório criado: {DIR_CLEAN}")
  }
  
  arq_processos  <- file.path(DIR_CLEAN, "df_processos.rds")
  arq_movimentos <- file.path(DIR_CLEAN, "df_movimentos.rds")
  arq_assuntos   <- file.path(DIR_CLEAN, "df_assuntos.rds")
  
  saveRDS(df_processos,  arq_processos)
  saveRDS(df_movimentos, arq_movimentos)
  saveRDS(df_assuntos,   arq_assuntos)
  
  log_info("OUTPUT | df_processos  -> {arq_processos}")
  log_info("OUTPUT | df_movimentos -> {arq_movimentos}")
  log_info("OUTPUT | df_assuntos   -> {arq_assuntos}")
}

# RESUMO ESTATÍSTICO DOS DATAFRAMES -------------------------------------------
log_info("SUMMARY | Gerando resumo estatístico dos dataframes")

resumo_processos <- df_processos |>
  summarise(
    n_processos        = n(),
    n_processos_uniq   = n_distinct(processo),
    n_tribunais        = n_distinct(tribunal),
    tribunal           = unique(tribunal),
    n_classes          = n_distinct(classe_nome),
    n_municipios       = n_distinct(orgao_julgador_municipio_ibge),
    ano_min            = min(ano_ajuizamento, na.rm = TRUE),
    ano_max            = max(ano_ajuizamento, na.rm = TRUE),
    data_min           = min(data_ajuizamento, na.rm = TRUE),
    data_max           = max(data_ajuizamento, na.rm = TRUE),
    pct_sem_hora       = mean(format(data_ajuizamento, "%H%M%S") == "000000", na.rm = TRUE)
  )

resumo_movimentos <- df_movimentos |>
  summarise(
    n_processos_uniq   = n_distinct(processo),
    n_movimentos       = n(),
    n_tipos_movimento  = n_distinct(movimento_codigo),
    data_min           = min(movimento_data_hora, na.rm = TRUE),
    data_max           = max(movimento_data_hora, na.rm = TRUE),
    media_mov_processo = n() / n_distinct(processo),
    pct_com_complemento = mean(!is.na(complemento_codigo))
  )

resumo_assuntos <- df_assuntos |>
  summarise(
    n_processos_uniq   = n_distinct(processo),
    n_assuntos         = n(),
    n_assuntos_uniq    = n_distinct(assunto_codigo)
  )

log_info("SUMMARY | df_processos:\n{paste(capture.output(glimpse(resumo_processos)), collapse = '\n')}")
log_info("SUMMARY | df_movimentos:\n{paste(capture.output(glimpse(resumo_movimentos)), collapse = '\n')}")
log_info("SUMMARY | df_assuntos:\n{paste(capture.output(glimpse(resumo_assuntos)), collapse = '\n')}")

# REMOVENDO OBJETOS DO GLOBAL ENV-----------------------------------------------
rm(
  con,
  resumo_intervalos,
  resumo_parse,
  resumo_assuntos,
  resumo_processos,
  resumo_movimentos,
  tabela_meianoite,
  tabela_municipios,
  tabela_na,
  tabela_qa_critica,
  tabela_qa_mov,
  agora_utc,
  codigos_ibge,
  ROOT,
  DIR_LOG,
  log_file,
  rproj_files,
  check_na_df,
  obter_dados_municipio_ibge,
  parse_data_ajuizamento_utc,
  perfil_data,
  qa_movimentos,
  qa_sem_na,
  check_parquet_parsed,
  via_source
)

# rm condicional: DIR_CLEAN e arq_* só existem se o usuário optou por salvar
if (exists("DIR_CLEAN"))      rm(DIR_CLEAN)
if (exists("arq_processos"))  rm(arq_processos)
if (exists("arq_movimentos")) rm(arq_movimentos)
if (exists("arq_assuntos"))   rm(arq_assuntos)

# FINALIZAÇÃO ------------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- round(difftime(end_time, start_time, units = "secs"), 2)
log_info("Pipeline encerrado com sucesso | tempo gasto: {elapsed}s | fim: {format(end_time, '%Y-%m-%d %H:%M:%S')}")
rm(end_time, start_time, elapsed)
#eof----------------------------------------------------------------------------