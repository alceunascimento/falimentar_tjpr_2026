#!/usr/bin/env Rscript
# bin/mni_to_parquet.R
# Converte arquivos JSON do MNI -> processos_mni.parquet
# Uso: Rscript bin/mni_to_parquet.R  (executado a partir da raiz do projeto)
# Leitura e compilação feitas inteiramente dentro do DuckDB (read_json glob + SQL)

## LOG AND ERROR CONTROL -------------------------------------------------------
start_time <- Sys.time()

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(logger)

DIR_LOG <- file.path(ROOT, "logs")
if (!dir.exists(DIR_LOG)) dir.create(DIR_LOG, recursive = TRUE)

log_file <- file.path(
  DIR_LOG,
  paste0("mni_to_parquet_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

log_threshold(DEBUG)
log_appender(appender_console,        index = 1)
log_layout(layout_glue_colors,        index = 1)
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,               index = 2)

log_info("Pipeline iniciado")

## BANNER INFORMATIVO ----------------------------------------------------------
cat("-----------------------------------------------------------------------------------\n")
cat("-------- MNI: EXECUTANDO RSCRIPT DE TRANSFORMACAO DOS JSON PARA PARQUET ----------\n")
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
# Uso normal : Rscript bin/mni_to_parquet.R
# Modo teste : Rscript bin/mni_to_parquet.R --test
args      <- commandArgs(trailingOnly = TRUE)
test_mode <- "--test" %in% args
if (test_mode) log_warn("SETUP | MODO TESTE ATIVO — lendo de data_mni/sample/")

## paths ----
log_info("SETUP | Resolvendo paths")

# commandArgs() captura o path do script quando invocado via Rscript
script_path  <- normalizePath(sub("--file=", "", grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)), winslash = "/")
PROJ_ROOT    <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/")
INPUT_SUBDIR <- if (test_mode) "sample" else "processed"
DIR_INPUT    <- file.path(PROJ_ROOT, "data_mni", INPUT_SUBDIR)
DIR_OUT      <- file.path(PROJ_ROOT, "data_mni")
FILE_OUT     <- file.path(DIR_OUT, if (test_mode) "processos_mni_test.parquet" else "processos_mni.parquet")
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

# Extensão JSON: necessária para read_json() com glob
tryCatch({
  dbExecute(con, "INSTALL json")
  dbExecute(con, "LOAD json")
  log_info("PROC | Extensão JSON carregada")
}, error = function(e) {
  log_error("PROC | Falha ao carregar extensão JSON: {conditionMessage(e)}")
  dbDisconnect(con, shutdown = TRUE)
  quit(save = "no", status = 1)
})

log_info("PROC | Executando read_json + extração de campos via SQL")

# DuckDB lê todos os *.json em paralelo via glob.
# Campos nested acessados com notação de ponto sobre as structs.
# filename_col=true injeta o caminho de origem para rastreabilidade.
query <- glue("
  COPY (
    SELECT
      -- metadados
      metadados.numero_processo                                    AS numero_processo,
      metadados.data_consulta                                      AS data_consulta,
      metadados.data_formatada                                     AS data_formatada,
      metadados.arquivo_origem                                     AS arquivo_origem,

      -- resumo
      resumo.classe_processual                                     AS classe_processual,
      resumo.data_ajuizamento                                      AS data_ajuizamento,
      TRY_CAST(resumo.valor_causa AS DOUBLE)                       AS valor_causa,
      resumo.orgao_julgador                                        AS orgao_julgador_nome,
      resumo.situacao                                              AS situacao,
      resumo.status                                                AS status,
      TRY_CAST(resumo.total_movimentos AS INTEGER)                 AS total_movimentos,
      resumo.ultimo_movimento.data                                 AS ultimo_movimento_data,
      resumo.ultimo_movimento.descricao                            AS ultimo_movimento_descricao,

      -- processo.dados_basicos (escalares)
      processo.dados_basicos.competencia                           AS competencia,
      processo.dados_basicos.codigoLocalidade                      AS codigo_localidade,
      (processo.dados_basicos.juizo100Digital = 'true')            AS juizo_100_digital,
      TRY_CAST(processo.dados_basicos.nivelSigilo AS INTEGER)      AS nivel_sigilo,
      TRY_CAST(processo.dados_basicos.custasRecolhidas AS DOUBLE)  AS custas_recolhidas,

      -- processo.dados_basicos.orgaoJulgador.atributos
      processo.dados_basicos.orgaoJulgador.atributos.codigoOrgao          AS codigo_orgao,
      processo.dados_basicos.orgaoJulgador.atributos.codigoMunicipioIBGE  AS codigo_municipio_ibge,
      processo.dados_basicos.orgaoJulgador.atributos.instancia            AS instancia

    FROM read_json('{GLOB}', auto_detect=true, union_by_name=true)
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

# EXPORTANDO CSV --------------------------------------------------------------
write_csv(pf, sub("\\.parquet$", ".csv", FILE_OUT))

# FINALIZAÇÃO ------------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- round(difftime(end_time, start_time, units = "secs"), 2)
log_info("Pipeline encerrado com sucesso | tempo gasto: {elapsed}s | fim: {format(end_time, '%Y-%m-%d %H:%M:%S')}")
rm(end_time, start_time, elapsed)
# eof --------------------------------------------------------------------------