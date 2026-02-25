#!/usr/bin/env Rscript
# --------------------------
# Parse JSON do Datajud usando DuckDB nativo
# Saída: processos.parquet, assuntos.parquet, movimentos.parquet
# --------------------------

# SETUP -------------------------------------------------------------------------
start_time <- Sys.time()
message(sprintf("[INFO] Início do script em %s", format(start_time, "%Y-%m-%d %H:%M:%S")))

# --- Parâmetros do projeto ---
PROJECT  <- normalizePath(getwd())
DATADIR  <- file.path(PROJECT, "data")
OUTDIR   <- file.path(PROJECT, "data", "parsed")

## 0) Pacotes -------------------------------------------------------------------
message("[INFO] Carregando pacotes...")

cran_pkgs <- c("duckdb", "DBI", "glue", "stringr")

if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman", repos = "https://cran-r.c3sl.ufpr.br/")
}
pacman::p_load(cran_pkgs, character.only = TRUE)

message("[OK] Pacotes carregados...")

# --- Preparação ---
dir.create(OUTDIR, recursive = TRUE, showWarnings = FALSE)

files <- list.files(
  DATADIR, 
  pattern = "^datajud_raw_.*\\.ndjson$", 
  full.names = TRUE
)

if(length(files) == 0L){
  message("[INFO] Nenhum arquivo JSON encontrado em: ", DATADIR)
  quit(save = "no", status = 0)
}

message(sprintf("[INFO] %d arquivos encontrados:\n  %s", 
                length(files), 
                paste(basename(files), collapse = "\n  ")))

## Inicializar DuckDB -----------------------------------------------------------
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
message("[OK] DuckDB conectado")

dbExecute(con, "SET memory_limit   = '45GB'")
message("[INFO] DuckDB memory_limit = 45GB")

dbExecute(con, "SET threads        = 4")
message("[INFO] DuckDB threads = 4")

# Instalar extensão JSON
tryCatch({
  dbExecute(con, "INSTALL json")
  dbExecute(con, "LOAD json")
  message("[OK] Extensão JSON carregada")
}, error = function(e) {
  message(sprintf("[ERROR] Falha ao carregar extensão JSON: %s", conditionMessage(e)))
  dbDisconnect(con, shutdown = TRUE)
  quit(save = "no", status = 1)
})


# Criar string com lista de arquivos para DuckDB
files_str <- paste(sprintf("'%s'", files), collapse = ", ")

## Parsear com DuckDB -----------------------------------------------------------
message("[INFO] DuckDB parseando JSON em paralelo...")

# Tabela de processos
tryCatch({
  dbExecute(con, glue("
    CREATE TABLE processos AS
    SELECT 
      id,
      numeroProcesso AS processo,
      classe.codigo AS classe_codigo,
      classe.nome AS classe_nome,
      dataAjuizamento AS data_ajuizamento,
      orgaoJulgador.codigo AS orgao_julgador_codigo,
      orgaoJulgador.nome AS orgao_julgador_nome,
      orgaoJulgador.codigoMunicipioIBGE AS orgao_julgador_municipio_ibge,
      tribunal,
      grau
    FROM read_json_auto([", files_str, "], maximum_object_size = ", MAX_JSON_BYTES, ")
  "))
  
  n_proc <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM processos")$n
  message(sprintf("[OK] %d processos parseados", n_proc))
  
}, error = function(e) {
  message(sprintf("[ERROR] Falha ao criar tabela processos: %s", conditionMessage(e)))
})

# Tabela de assuntos
tryCatch({
  dbExecute(con, glue("
    CREATE TABLE assuntos AS
    SELECT 
      id,
      numeroProcesso AS processo,
      assunto.codigo AS assunto_codigo,
      REPLACE(assunto.nome, '\"', '') AS assunto_nome
    FROM read_json_auto([", files_str, "], maximum_object_size = ", MAX_JSON_BYTES, "),
    UNNEST(assuntos) AS t(assunto)
  "))
  
  n_ass <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM assuntos")$n
  message(sprintf("[OK] %d assuntos parseados", n_ass))
  
}, error = function(e) {
  message(sprintf("[ERROR] Falha ao criar tabela assuntos: %s", conditionMessage(e)))
})


# Tabela de movimentos COM complementos desaninhados
tryCatch({
  dbExecute(con, glue("
    CREATE TABLE movimentos AS
    SELECT 
      id,
      numeroProcesso AS processo,
      movimento.codigo AS movimento_codigo,
      REPLACE(movimento.nome, '\"', '') AS movimento_nome,
      movimento.dataHora AS movimento_data_hora,
      complemento.codigo AS complemento_codigo,
      REPLACE(complemento.nome, '\"', '') AS complemento_nome,
      REPLACE(CAST(complemento.valor AS VARCHAR), '\"', '') AS complemento_valor,
      REPLACE(complemento.descricao, '\"', '') AS complemento_descricao
    FROM read_json_auto([", files_str, "], maximum_object_size = ", MAX_JSON_BYTES, "),
    UNNEST(movimentos) AS t(movimento)
    LEFT JOIN UNNEST(movimento.complementosTabelados) AS c(complemento) ON true
  "))
  
  n_mov <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM movimentos")$n
  message(sprintf("[OK] %d movimentos parseados", n_mov))
  
}, error = function(e) {
  message(sprintf("[ERROR] Falha ao criar tabela movimentos: %s", conditionMessage(e)))
})



## Exportar para Parquet --------------------------------------------------------
message("[INFO] Exportando para Parquet...")

dbExecute(con, glue("COPY processos TO '{OUTDIR}/processos.parquet' (FORMAT PARQUET)"))
message(sprintf("[OK] Exportado: {OUTDIR}/processos.parquet"))

dbExecute(con, glue("COPY assuntos TO '{OUTDIR}/assuntos.parquet' (FORMAT PARQUET)"))
message(sprintf("[OK] Exportado: {OUTDIR}/assuntos.parquet"))

dbExecute(con, glue("COPY movimentos TO '{OUTDIR}/movimentos.parquet' (FORMAT PARQUET)"))
message(sprintf("[OK] Exportado: {OUTDIR}/movimentos.parquet"))

## Limpeza ----------------------------------------------------------------------
dbDisconnect(con, shutdown = TRUE)
message("[DONE] Parsing concluído.")

end_time <- Sys.time()
message(sprintf(
  "[INFO] Script finalizado. Duração: %.2f minutos",
  as.numeric(difftime(end_time, start_time, units = "mins"))
))
#eof----------------------------------------------------------------------------