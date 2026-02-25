#!/usr/bin/env Rscript
# bin/mni_movimentos_to_parquet.R
# Expande lista processo.movimentos dos JSON do MNI -> movimentos_mni.parquet
# Cada movimento vira uma observação (UNNEST); complementos parseados posicionalmente
# Uso: Rscript bin/mni_movimentos_to_parquet.R
#      Rscript bin/mni_movimentos_to_parquet.R --test

## LOG AND ERROR CONTROL -------------------------------------------------------
start_time <- Sys.time()

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(logger)

DIR_LOG <- file.path(ROOT, "logs")
if (!dir.exists(DIR_LOG)) dir.create(DIR_LOG, recursive = TRUE)

log_file <- file.path(
  DIR_LOG,
  paste0("mni_movimentos_to_parquet_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

log_threshold(DEBUG)
log_appender(appender_console,        index = 1)
log_layout(layout_glue_colors,        index = 1)
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,               index = 2)

log_info("Pipeline iniciado")

## BANNER INFORMATIVO ----------------------------------------------------------
cat("-----------------------------------------------------------------------------------\n")
cat("-------- MNI: EXPANDINDO MOVIMENTOS DOS JSON PARA PARQUET ------------------------\n")
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
cat("-----------------------------------------------------------------------------------\n\n")

# SETUP ------------------------------------------------------------------------
log_info("SETUP | Iniciando configuração")

## pacotes ----
log_info("SETUP | Carregando pacotes via pacman")
log_warn("SETUP | Pacotes ausentes serão instalados automaticamente (pode demorar)")
log_warn("SETUP | Se tiver que instalar o duckdb vai demorar bastante, aguarde com paciência")

pacman::p_load(duckdb, arrow, glue, readr)

log_info("SETUP | Pacotes carregados")

## argumentos de linha de comando ----
# Uso normal : Rscript bin/mni_movimentos_to_parquet.R
# Modo teste : Rscript bin/mni_movimentos_to_parquet.R --test
args      <- commandArgs(trailingOnly = TRUE)
test_mode <- "--test" %in% args
if (test_mode) log_warn("SETUP | MODO TESTE ATIVO — lendo de data_mni/sample/")

## paths ----
log_info("SETUP | Resolvendo paths")

script_path  <- normalizePath(sub("--file=", "", grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)), winslash = "/")
PROJ_ROOT    <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/")
INPUT_SUBDIR <- if (test_mode) "sample" else "processed"
DIR_INPUT    <- file.path(PROJ_ROOT, "data_mni", INPUT_SUBDIR)
DIR_OUT      <- file.path(PROJ_ROOT, "data_mni")
FILE_OUT     <- file.path(DIR_OUT, if (test_mode) "movimentos_mni_test.parquet" else "movimentos_mni.parquet")
GLOB         <- file.path(DIR_INPUT, "*.json")

if (!dir.exists(DIR_OUT)) {
  dir.create(DIR_OUT, recursive = TRUE)
  log_info("SETUP | Diretório de saída criado: {DIR_OUT}")
}
if (!dir.exists(DIR_INPUT)) {
  log_error("SETUP | Diretório de entrada não encontrado: {DIR_INPUT}")
  stop("Diretório de entrada não encontrado: ", DIR_INPUT)
}

n_arq <- length(list.files(DIR_INPUT, pattern = "\\.json$"))
if (n_arq == 0) {
  log_error("SETUP | Nenhum arquivo .json encontrado em {DIR_INPUT}")
  stop("Nenhum arquivo .json encontrado.")
}

log_info("SETUP | DIR_INPUT : {DIR_INPUT}")
log_info("SETUP | DIR_OUT   : {DIR_OUT}")
log_info("SETUP | FILE_OUT  : {FILE_OUT}")
log_info("SETUP | Arquivos .json encontrados: {n_arq}")

# PROCESSAMENTO ----------------------------------------------------------------
log_info("PROC | Iniciando conexão DuckDB")

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
log_info("PROC | DuckDB conectado (in-memory)")

# memory_limit: 80% da RAM física detectada via benchmarkme (cross-platform)
pacman::p_load(benchmarkme)
ram_gb <- tryCatch({
  round(as.numeric(get_ram()) / 1024^3 * 0.8, 1)
}, error = function(e) {
  log_warn("PROC | Falha ao detectar RAM via benchmarkme; usando fallback 80% de 64GB = 51.2GB")
  51.2
})

n_threads <- parallel::detectCores()
mem_limit <- glue("{ram_gb}GB")

dbExecute(con, glue("SET memory_limit = '{mem_limit}'"))
dbExecute(con, glue("SET threads      = {n_threads}"))
log_info("PROC | memory_limit = {mem_limit} | threads = {n_threads}")

# Extensão JSON
tryCatch({
  dbExecute(con, "INSTALL json")
  dbExecute(con, "LOAD json")
  log_info("PROC | Extensão JSON carregada")
}, error = function(e) {
  log_error("PROC | Falha ao carregar extensão JSON: {conditionMessage(e)}")
  dbDisconnect(con, shutdown = TRUE)
  quit(save = "no", status = 1)
})

log_info("PROC | Executando UNNEST de movimentos via SQL")

# Lógica dos complementos (sempre VARCHAR[]):
#   complementos[1]            → complemento_projudi_nome      (sempre o nome do movimento)
#   complementos[2 .. n-1]     → complemento_projudi_descricao (0, 1 ou 2 itens intermediários; concatenados com ' | ')
#   complementos[n]            → complemento_projudi_movimentado_por  (sempre "Movimentada Por:...")
#
# list_slice(arr, 2, len-1): itens intermediários (vazio quando len <= 2)
# array_to_string(..., ' | '): concatena múltiplos itens de descrição em string única
#
# UNNEST(processo.movimentos): expande o array — cada elemento vira uma linha,
# mantendo numero_processo como chave de junção com processos_mni.parquet

query <- glue("
  COPY (
    SELECT
      -- chave de junção com processos_mni.parquet
      metadados.numero_processo                                             AS numero_processo,

      -- identificação do movimento
      mv.atributos.identificadorMovimento                                   AS movimento_sequencia,
      mv.dataHoraFormatada                                                  AS movimento_data_hora,
      mv.atributos.dataHora                                                 AS movimento_data_hora_raw,
      mv.atributos.classeProcessual                                         AS classe_processual,

      -- código e nome nacional do movimento (tabela CNJ)
      mv.movimentoNacional.atributos.codigoNacional                         AS movimento_codigo,
      mv.movimentoNacional.complementos[1]                                  AS movimento_nome,

      -- complementos do ProjUDI (sistema local TJPR)
      mv.complementos[1]                                                    AS complemento_projudi_nome,

      CASE
        WHEN len(mv.complementos) <= 2 THEN NULL
        ELSE array_to_string(list_slice(mv.complementos, 2, len(mv.complementos) - 1), ' | ')
      END                                                                   AS complemento_projudi_descricao,

      mv.complementos[len(mv.complementos)]                                                    AS complemento_projudi_movimentado_por,

      -- parse de descricao:nome - cargo, 3 campos normalizados em lower case
      lower(trim(string_split(mv.complementos[len(mv.complementos)], ':')[1]))                 AS complemento_projudi_movimentado_por_descricao,
      lower(trim(string_split(string_split(mv.complementos[len(mv.complementos)], ':')[2], ' - ')[1]))  AS complemento_projudi_movimentado_por_nome,
      lower(trim(string_split(string_split(mv.complementos[len(mv.complementos)], ':')[2], ' - ')[2]))  AS complemento_projudi_movimentado_por_cargo

    FROM read_json('{GLOB}', auto_detect=true, union_by_name=true),
         UNNEST(processo.movimentos) AS t(mv)
  )
  TO '{FILE_OUT}' (FORMAT PARQUET, COMPRESSION ZSTD)
")

dbExecute(con, query)
dbDisconnect(con, shutdown = TRUE)

log_info("PROC | Query DuckDB executada com sucesso")

# VALIDAÇÃO --------------------------------------------------------------------
log_info("VALID | Lendo parquet para validação de schema")

pf     <- read_parquet(FILE_OUT)
n_rows <- nrow(pf)
n_cols <- ncol(pf)
sz_kb  <- round(file.size(FILE_OUT) / 1024, 1)

log_info("VALID | Dimensões  : {n_rows} linhas x {n_cols} colunas")
log_info("VALID | Tamanho    : {sz_kb} KB")
log_info("VALID | Colunas    : {paste(names(pf), collapse = ', ')}")

# EXPORTA CSV ------------------------------------------------------------------
write_csv(pf, sub("\\.parquet$", ".csv", FILE_OUT))

# FINALIZAÇÃO ------------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- round(difftime(end_time, start_time, units = "secs"), 2)
log_info("Pipeline encerrado com sucesso | tempo gasto: {elapsed}s | fim: {format(end_time, '%Y-%m-%d %H:%M:%S')}")
rm(end_time, start_time, elapsed)
# eof --------------------------------------------------------------------------