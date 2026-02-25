#!/usr/bin/env Rscript
# bin/mni_movimentos_parse_partes.R
# Lê movimentos_mni.parquet e extrai campos semânticos de complemento_projudi_descricao
# Input : data_mni/movimentos_mni.parquet  (ou movimentos_mni_test.parquet em --test)
# Output: data_mni/movimentos_mni_partes.parquet  +  data_mni/partes_mni.parquet
# Uso   : Rscript bin/mni_movimentos_parse_partes.R
#         Rscript bin/mni_movimentos_parse_partes.R --test

## LOG AND ERROR CONTROL -------------------------------------------------------
start_time <- Sys.time()

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(logger)

DIR_LOG <- file.path(ROOT, "logs")
if (!dir.exists(DIR_LOG)) dir.create(DIR_LOG, recursive = TRUE)

log_file <- file.path(
  DIR_LOG,
  paste0("mni_movimentos_parse_partes_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

log_threshold(DEBUG)
log_appender(appender_console,        index = 1)
log_layout(layout_glue_colors,        index = 1)
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,               index = 2)

log_info("Pipeline iniciado")

## BANNER INFORMATIVO ----------------------------------------------------------
cat("-----------------------------------------------------------------------------------\n")
cat("-------- MNI: PARSING SEMÂNTICO DE PARTES EM MOVIMENTOS_MNI.PARQUET --------------\n")
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

pacman::p_load(duckdb, glue, readr)
log_info("SETUP | Pacotes carregados")

args      <- commandArgs(trailingOnly = TRUE)
test_mode <- "--test" %in% args
if (test_mode) log_warn("SETUP | MODO TESTE ATIVO — usando movimentos_mni_test.parquet")

script_path <- normalizePath(
  sub("--file=", "", grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)),
  winslash = "/"
)
PROJ_ROOT <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/")
DIR_DATA  <- file.path(PROJ_ROOT, "data_mni")

FILE_IN     <- file.path(DIR_DATA, if (test_mode) "movimentos_mni_test.parquet"        else "movimentos_mni.parquet")
FILE_OUT    <- file.path(DIR_DATA, if (test_mode) "movimentos_mni_partes_test.parquet"  else "movimentos_mni_partes.parquet")
FILE_PARTES <- file.path(DIR_DATA, if (test_mode) "partes_mni_test.parquet"             else "partes_mni.parquet")

if (!file.exists(FILE_IN)) {
  log_error("SETUP | Arquivo de entrada não encontrado: {FILE_IN}")
  stop("Arquivo não encontrado: ", FILE_IN)
}

log_info("SETUP | FILE_IN     : {FILE_IN}")
log_info("SETUP | FILE_OUT    : {FILE_OUT}")
log_info("SETUP | FILE_PARTES : {FILE_PARTES}")

# PROCESSAMENTO ----------------------------------------------------------------
log_info("PROC | Iniciando conexão DuckDB")

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

pacman::p_load(benchmarkme)
ram_gb <- tryCatch(
  round(as.numeric(get_ram()) / 1024^3 * 0.8, 1),
  error = function(e) { log_warn("PROC | RAM não detectada; fallback 51.2GB"); 51.2 }
)
n_threads <- parallel::detectCores()
mem_limit <- glue("{ram_gb}GB")

dbExecute(con, glue("SET memory_limit = '{mem_limit}'"))
dbExecute(con, glue("SET threads      = {n_threads}"))
log_info("PROC | memory_limit = {mem_limit} | threads = {n_threads}")

# ── LÓGICA DOS REGEX (validada em Python contra 13 casos reais) ─────────────
#
# REGRA FUNDAMENTAL:
#   administrador_judicial SÓ é preenchido quando o texto contém o padrão
#   HAB/DESAB/DESHAB...ILITAÇÃO DE PARTE EM PROCESSO, OU quando o destinatário
#   da intimação é identificado como SÍNDICO DO(A).
#   MASSA FALIDA no texto de intimação = nome da parte — NÃO é indicador de AJ.
#
# parte_nome — COALESCE em dois ramos mutuamente exclusivos:
#
#   RAMO 1 — texto é HAB/DESAB (ancora em "(?:DESH?)?ABILIT.+?Parte:"):
#     Sub-A: "ADMINISTRADOR JUDICIAL (nome) (especie)"  → nome entre ()
#     Sub-B: "(ADMINISTRADOR JUDICIAL) nome (especie)"  → nome após prefixo
#     Sub-C: "nome (SÍNDICO DO(A) empresa) (especie)"   → nome antes do (SÍNDICO
#     Sub-D: "nome (ADMINISTRADOR JUDICIAL) (especie)"  → nome antes do (AJ
#     Sub-E: simples "nome (especie)"                   → inclui MASSA FALIDA sem AJ
#
#   RAMO 2 — texto é intimação/leitura (Para advog*/P/ advgs./Pelo advogado):
#     Captura nome até "com prazo" | "-" | "em DD"; mantém bloco "(SÍNDICO DO(A)...)"
#     intacto para que PAT_SINDICO o decomponha em nome + empresa
#
# parte_especie — último "(especie)" ancorado no fim; NULL fora de HAB/DESAB
#
# administrador_judicial:
#   HAB/DESAB: preenchido com nome da pessoa nos sub-padrões A, B, C, D (não em E simples)
#   ADVOG+SÍNDICO: preenchido quando SÍNDICO DO(A) está no nome do destinatário

ESPECIES <- "Promovente|Promovido|Terceiro|Réu|Reus|Autor|Executado|Exequente"

# Âncora de HAB/DESAB — cobre HABILITAÇÃO, DESABILITAÇÃO, DESHABILITAÇÃO
# (?:DESH?)?ABILIT casas as três variantes sem depender de acentos literais
PAT_HAB_ANCORA <- "(?i)(?:DESH?)?ABILIT.+?Parte:\\s+"

query_enrich <- glue("
  COPY (
    SELECT
      *,

      -- ── parte_nome ──────────────────────────────────────────────────────────
      -- Pós-processamento final: remove prefixo MASSA FALIDA DE/- e colapsa espaços
      lower(trim(regexp_replace(
        regexp_replace(
          regexp_replace(
            CASE
              -- RAMO 1: HAB/DESAB
              WHEN regexp_matches(complemento_projudi_descricao, '{PAT_HAB_ANCORA}') THEN
                COALESCE(
                  -- Sub-A: ADMINISTRADOR JUDICIAL (nome)
                  NULLIF(regexp_extract(complemento_projudi_descricao,
                    '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+ADMINISTRADOR\\s+JUDICIAL\\s*\\(([^)]+)\\)\\s*\\(',
                    1), ''),
                  -- Sub-B: (ADMINISTRADOR JUDICIAL) nome
                  NULLIF(regexp_extract(complemento_projudi_descricao,
                    '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+\\(ADMINISTRADOR\\s+JUDICIAL\\)\\s*([^(]+?)\\s*\\(',
                    1), ''),
                  -- Sub-C: nome (SÍNDICO DO(A) empresa)
                  NULLIF(regexp_extract(complemento_projudi_descricao,
                    '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+(.+?)\\s*\\(S[I\\xcd]NDICO\\s+DO\\(A\\)',
                    1), ''),
                  -- Sub-D: nome (ADMINISTRADOR JUDICIAL)
                  NULLIF(regexp_extract(complemento_projudi_descricao,
                    '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+([^(]+?)\\s*\\((?:ADMINISTRADOR\\s+JUDICIAL|Administrador\\s+Judicial)\\)\\s*\\(',
                    1), ''),
                  -- Sub-E: simples nome (especie)
                  NULLIF(regexp_extract(complemento_projudi_descricao,
                    '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+(.+?)\\s*\\((?:{ESPECIES})\\)',
                    1), '')
                )
              -- RAMO 2: intimação/leitura para advog*/curador/defensor
              WHEN regexp_matches(complemento_projudi_descricao,
                '(?i)(?:[Pp]/\\s*advgs\\.|[Aa]dvogados?/curador/defensor|[Pp]elo\\s+advogado/curador/defensor)\\s+de\\s+') THEN
                NULLIF(regexp_replace(
                  regexp_extract(complemento_projudi_descricao,
                    '(?i)(?:[Pp]/\\s*advgs\\.|[Aa]dvogados?/curador/defensor|[Pp]elo\\s+advogado/curador/defensor)\\s+de\\s+(.+?(?:\\([^)]+\\))?)(?:\\s+com\\s+prazo|\\s+\\*|\\s+em\\s+\\d|\\s*\\)\\s+em\\s|\\s*\\z)',
                    1),
                  '(?i)\\s*\\(S[I\\xcd]NDICO\\s+DO\\(A\\).+\\z', ''),
                '')
              ELSE NULL
            END,
            '\\s+', ' '
          ),
          -- remove prefixo MASSA FALIDA DE/- mantendo apenas o nome da empresa/pessoa
          '(?i)^MASSA\\s+FALIDA\\s*(?:DE\\s+|-\\s*)', ''
        ),
        '\\s+', ' '
      )))                                               AS complemento_projudi_descricao_parte_nome,

      -- ── parte_especie ────────────────────────────────────────────────────────
      -- Presente apenas em HAB/DESAB; NULL nos demais
      CASE
        WHEN regexp_matches(complemento_projudi_descricao, '{PAT_HAB_ANCORA}') THEN
            lower(regexp_extract(complemento_projudi_descricao,
              '(?i)\\(({ESPECIES})\\)\\s*\\z', 1))
        ELSE NULL
      END                                               AS complemento_projudi_descricao_parte_especie,

      -- ── administrador_judicial ───────────────────────────────────────────────
      -- Preenchido apenas quando:
      --   (a) HAB/DESAB com AJ/SÍNDICO explícito (sub-padrões A, B, C, D)
      --   (b) intimação cujo destinatário é identificado como SÍNDICO DO(A)
      -- MASSA FALIDA em intimação simples → NULL (é nome de parte, não cargo)
      CASE
        -- (a) HAB/DESAB sub-C: nome (SÍNDICO DO(A) empresa) → nome do síndico
        WHEN regexp_matches(complemento_projudi_descricao,
            '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+.+?\\(S[I\\xcd]NDICO\\s+DO\\(A\\)') THEN
            lower(trim(regexp_extract(complemento_projudi_descricao,
              '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+(.+?)\\s*\\(S[I\\xcd]NDICO\\s+DO\\(A\\)', 1)))
        -- (a) HAB/DESAB sub-A: ADMINISTRADOR JUDICIAL (nome) → nome
        WHEN regexp_matches(complemento_projudi_descricao,
            '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+ADMINISTRADOR\\s+JUDICIAL\\s*\\(') THEN
            lower(trim(regexp_extract(complemento_projudi_descricao,
              '(?i)ADMINISTRADOR\\s+JUDICIAL\\s*\\(([^)]+)\\)', 1)))
        -- (a) HAB/DESAB sub-B: (ADMINISTRADOR JUDICIAL) nome → nome
        WHEN regexp_matches(complemento_projudi_descricao,
            '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+\\(ADMINISTRADOR\\s+JUDICIAL\\)') THEN
            lower(trim(regexp_extract(complemento_projudi_descricao,
              '(?i)\\(ADMINISTRADOR\\s+JUDICIAL\\)\\s*([^(]+?)\\s*\\(', 1)))
        -- (a) HAB/DESAB sub-D: nome (ADMINISTRADOR JUDICIAL) → nome
        WHEN regexp_matches(complemento_projudi_descricao,
            '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+[^(]+\\((?:ADMINISTRADOR\\s+JUDICIAL|Administrador\\s+Judicial)\\)') THEN
            lower(trim(regexp_extract(complemento_projudi_descricao,
              '(?i)(?:DESH?)?ABILIT.+?Parte:\\s+([^(]+?)\\s*\\((?:ADMINISTRADOR\\s+JUDICIAL|Administrador\\s+Judicial)\\)', 1)))
        -- (b) intimação para SÍNDICO DO(A) → nome do síndico
        WHEN regexp_matches(complemento_projudi_descricao,
            '(?i)(?:[Pp]/\\s*advgs\\.|[Aa]dvogados?/curador/defensor|[Pp]elo\\s+advogado/curador/defensor)\\s+de\\s+.+?S[I\\xcd]NDICO\\s+DO\\(A\\)') THEN
            lower(trim(regexp_extract(complemento_projudi_descricao,
              '(?i)(?:[Pp]/\\s*advgs\\.|[Aa]dvogados?/curador/defensor|[Pp]elo\\s+advogado/curador/defensor)\\s+de\\s+(.+?)\\s*\\(S[I\\xcd]NDICO\\s+DO\\(A\\)', 1)))
        ELSE NULL
      END                                               AS complemento_projudi_descricao_administrador_judicial

    FROM read_parquet('{FILE_IN}')
  )
  TO '{FILE_OUT}' (FORMAT PARQUET, COMPRESSION ZSTD)
")

dbExecute(con, query_enrich)
log_info("PROC | Parsing concluído: {FILE_OUT}")

# PARTES_MNI -------------------------------------------------------------------
log_info("PARTES | Gerando partes_mni.parquet")

query_partes <- glue("
  COPY (
    SELECT
      numero_processo,
      classe_processual                                      AS classe_codigo,
      movimento_sequencia,
      movimento_data_hora,
      movimento_codigo,
      movimento_nome,
      complemento_projudi_descricao_parte_nome               AS parte_nome,
      complemento_projudi_descricao_parte_especie            AS parte_especie,
      complemento_projudi_descricao_administrador_judicial   AS administrador_judicial,
      complemento_projudi_descricao,
      complemento_projudi_movimentado_por_nome,
      complemento_projudi_movimentado_por_cargo
    FROM read_parquet('{FILE_OUT}')
    WHERE complemento_projudi_descricao_parte_nome               IS NOT NULL
       OR complemento_projudi_descricao_parte_especie            IS NOT NULL
       OR complemento_projudi_descricao_administrador_judicial   IS NOT NULL
  )
  TO '{FILE_PARTES}' (FORMAT PARQUET, COMPRESSION ZSTD)
")

dbExecute(con, query_partes)
dbDisconnect(con, shutdown = TRUE)
log_info("PARTES | Arquivo gerado: {FILE_PARTES}")

# NORMALIZAÇÃO DE SUFIXOS SOCIETÁRIOS -----------------------------------------
# Nova conexão DuckDB para operar sobre FILE_PARTES — o Windows não permite
# write_parquet em arquivo com mmap ativo da conexão anterior
log_info("NORM | Normalizando sufixos societários em parte_nome")

pacman::p_load(arrow, dplyr, stringr)

FILE_PARTES_TMP <- paste0(tools::file_path_sans_ext(FILE_PARTES), "_tmp.parquet")

con_norm <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")
dbExecute(con_norm, glue("SET memory_limit = '{mem_limit}'"))
dbExecute(con_norm, glue("SET threads      = {n_threads}"))

pf_partes <- read_parquet(FILE_PARTES)
dbDisconnect(con_norm, shutdown = TRUE)

pf_partes <- pf_partes |>
  mutate(parte_nome = parte_nome |>
    str_replace_all(regex("\\s*&\\s*cia\\.?\\s+ltda\\.?", ignore_case = TRUE), " e cia ltda") |>
    str_replace_all(regex("\\s*&\\s*cia\\.?\\s+s\\.?/?a\\.?",  ignore_case = TRUE), " e cia sa")  |>
    str_replace_all(regex("\\bcia\\.",                                  ignore_case = TRUE), "cia")        |>
    str_replace_all(regex("\\bs\\.?/?a\\.?",                         ignore_case = TRUE), "sa")         |>
    str_replace_all(regex("\\bltda\\.?",                               ignore_case = TRUE), "ltda")       |>
    str_replace_all(",", "") |>
	str_squish()
  )

write_parquet(pf_partes, FILE_PARTES_TMP, compression = "zstd")
file.rename(FILE_PARTES_TMP, FILE_PARTES)
write_csv(pf_partes, sub("\\.parquet$", ".csv", FILE_PARTES))

log_info("NORM | parte_nome normalizado e partes_mni.parquet regravado")

# VALIDAÇÃO --------------------------------------------------------------------
log_info("VALID | Lendo parquets para validação")

pacman::p_load(arrow, dplyr)

pf       <- read_parquet(FILE_OUT)
n_rows   <- nrow(pf)
n_nome   <- sum(!is.na(pf$complemento_projudi_descricao_parte_nome)  & pf$complemento_projudi_descricao_parte_nome  != "")
n_esp    <- sum(!is.na(pf$complemento_projudi_descricao_parte_especie) & pf$complemento_projudi_descricao_parte_especie != "")
n_aj     <- sum(!is.na(pf$complemento_projudi_descricao_administrador_judicial))
sz_kb    <- round(file.size(FILE_OUT) / 1024, 1)

log_info("VALID | movimentos_mni_partes — {n_rows} linhas | {sz_kb} KB")
log_info("VALID | parte_nome preenchido           : {n_nome} ({round(n_nome/n_rows*100,1)}%)")
log_info("VALID | parte_especie preenchido        : {n_esp}  ({round(n_esp/n_rows*100,1)}%)")
log_info("VALID | administrador_judicial detectado: {n_aj}   ({round(n_aj/n_rows*100,1)}%)")

pf_partes <- read_parquet(FILE_PARTES)
sz_partes <- round(file.size(FILE_PARTES) / 1024, 1)
log_info("VALID | partes_mni — {nrow(pf_partes)} linhas | {sz_partes} KB")
log_info("VALID | partes_mni — colunas: {paste(names(pf_partes), collapse = ', ')}")

# FINALIZAÇÃO ------------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- round(difftime(end_time, start_time, units = "secs"), 2)
log_info("Pipeline encerrado com sucesso | tempo gasto: {elapsed}s | fim: {format(end_time, '%Y-%m-%d %H:%M:%S')}")
rm(end_time, start_time, elapsed)
# eof --------------------------------------------------------------------------