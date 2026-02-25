#!/usr/bin/env Rscript
# bin/api_tpu_movimentos.R
#
# Enriquecimento via API CNJ-TPU
# Complementa df_movimentos com a hierarquia completa de cada movimento,
# subindo a cadeia cod_item_pai até o nó raiz (Magistrado id=1 / Serventuário id=14).
#
# Input : data/cleaned/df_movimentos.rds
# Output: data/cleaned/df_movimentos_tpu.rds
# Uso   : Rscript bin/api_tpu_movimentos.R
#
# ── HIERARQUIA TPU ─────────────────────────────────────────────────────────────
# Nível 1 (raiz): id=1  "Magistrado"  |  id=14 "Serventuário"
# Nível 2: subtipos (Decisão=3, Julgamento=193, Despacho=11009, ...)
# Nível 3: especializações do nível 2
# Nível 4: especializações do nível 3
# Nível 5: folhas
#
# Schema adicionado ao df_movimentos:
#   movimento_codigo_1..5  INTEGER  — id em cada nível da hierarquia (1=raiz)
#   movimento_nome_1..5    VARCHAR  — nome em cada nível
#   tpu_id                 INTEGER  — id do nó folha (= movimento_codigo original)
#   tpu_cod_item_pai       INTEGER  — cod_item_pai do nó folha
#   tpu_nome               VARCHAR  — nome do nó folha
#   tpu_descricao_glossario VARCHAR
#   tpu_glossario          VARCHAR
#   tpu_norma              VARCHAR
#   tpu_artigo             VARCHAR
#   tpu_dispositivo_legal  VARCHAR
#   tpu_data_inclusao      VARCHAR
#   tpu_data_versao        VARCHAR
#   tpu_data_alteracao     VARCHAR
# ──────────────────────────────────────────────────────────────────────────────

## LOG AND ERROR CONTROL -------------------------------------------------------
start_time <- Sys.time()

ROOT <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(logger)

DIR_LOG <- file.path(ROOT, "logs")
if (!dir.exists(DIR_LOG)) dir.create(DIR_LOG, recursive = TRUE)

log_file <- file.path(
  DIR_LOG,
  paste0("api_tpu_movimentos_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

log_threshold(DEBUG)
log_appender(appender_console,        index = 1)
log_layout(layout_glue_colors,        index = 1)
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,               index = 2)

log_info("Pipeline iniciado")

cat("-----------------------------------------------------------------------------------\n")
cat("-------- DATAJUD: ENRIQUECIMENTO TPU → df_movimentos_tpu.rds -------------------\n")
cat("-----------------------------------------------------------------------------------\n")
cat(sprintf("Início do script em %s\n", format(start_time, "%Y-%m-%d %H:%M:%S")))
cat("-----------------------------------------------------------------------------------\n")

rproj_files <- list.files(ROOT, pattern = "\\.Rproj$", full.names = TRUE)
if (length(rproj_files) == 0) {
  log_error("SETUP | Nenhum .Rproj encontrado em: {ROOT}")
  stop("Execute a partir da raiz do projeto.")
}
cat("Projeto detectado: ", basename(rproj_files[1]), "\n")
cat("Log de execução  : ", log_file, "\n")
cat("-----------------------------------------------------------------------------------\n\n")

## SETUP -----------------------------------------------------------------------
log_info("SETUP | Carregando pacotes")
pacman::p_load(dplyr, purrr, httr2, jsonlite, readr, glue, tibble)
log_info("SETUP | Pacotes carregados")

script_path <- normalizePath(
  sub("--file=", "", grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)),
  winslash = "/"
)
PROJ_ROOT  <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/")
DIR_DATA   <- file.path(PROJ_ROOT, "data", "cleaned")
FILE_IN    <- file.path(DIR_DATA, "df_movimentos.rds")
FILE_OUT   <- file.path(DIR_DATA, "df_movimentos_tpu.rds")
FILE_CACHE <- file.path(DIR_DATA, "tpu_cache.rds")  # cache local de nós TPU

if (!file.exists(FILE_IN)) {
  log_error("SETUP | Arquivo não encontrado: {FILE_IN}")
  stop("Arquivo não encontrado: ", FILE_IN)
}

log_info("SETUP | FILE_IN    : {FILE_IN}")
log_info("SETUP | FILE_OUT   : {FILE_OUT}")
log_info("SETUP | FILE_CACHE : {FILE_CACHE}")

## CACHE LOCAL -----------------------------------------------------------------
# Persiste em disco para evitar chamadas redundantes entre execuções.
# Chave: id do nó TPU (integer). Valor: lista com os campos da API.
if (file.exists(FILE_CACHE)) {
  tpu_cache <- readRDS(FILE_CACHE)
  log_info("CACHE | Carregado com {length(tpu_cache)} entradas")
} else {
  tpu_cache <- list()
  log_info("CACHE | Inicializado vazio")
}

## FUNÇÕES DE ACESSO À API TPU -------------------------------------------------

TPU_BASE <- "https://gateway.cloud.pje.jus.br/tpu/api/v1/publico/consulta/detalhada/movimentos"
CAMPOS_TPU <- c("id", "cod_item_pai", "nome", "descricao_glossario", "glossario",
                "norma", "artigo", "dispositivoLegal", "data_inclusao",
                "data_versao", "data_alteracao")

# fetch_tpu_node(): consulta a API para um único código e retorna lista com campos.
# Usa tpu_cache para evitar chamadas repetidas; atualiza o cache em memória.
# Sentinel: códigos inválidos/vazios são gravados como NA no cache para não
# serem requisitados novamente (evita chamadas duplicadas observadas no log).
fetch_tpu_node <- function(codigo) {
  key <- as.character(codigo)

  # Cache hit — retorna imediatamente (incluindo NA para códigos inválidos)
  if (key %in% names(tpu_cache)) {
    val <- tpu_cache[[key]]
    return(if (identical(val, NA)) NULL else val)
  }

  Sys.sleep(0.1)  # rate limit conservador

  resp <- tryCatch(
    request(TPU_BASE) |>
      req_url_query(codigo = codigo) |>
      req_headers(accept = "*/*") |>
      req_timeout(15) |>
      req_retry(max_tries = 3, backoff = ~2) |>
      req_perform(),
    error = function(e) {
      log_warn("API | ERRO ao consultar codigo={codigo}: {conditionMessage(e)}")
      NULL
    }
  )

  if (is.null(resp) || resp_status(resp) != 200) {
    log_warn("API | Resposta inválida para codigo={codigo} — gravando sentinel no cache")
    tpu_cache[[key]] <<- NA
    return(NULL)
  }

  dados <- resp_body_json(resp)
  if (length(dados) == 0) {
    log_warn("API | Resposta vazia para codigo={codigo} — gravando sentinel no cache")
    tpu_cache[[key]] <<- NA
    return(NULL)
  }

  no <- dados[[1]]
  resultado <- map(CAMPOS_TPU, ~ no[[.x]]) |> setNames(CAMPOS_TPU)

  tpu_cache[[key]] <<- resultado
  resultado
}

# get_hierarquia(): sobe a cadeia cod_item_pai até o nó raiz (cod_item_pai = NULL).
# Retorna um data.frame de linha única com os campos hierárquicos.
#   movimento_codigo_1 = id do nível 1 (Magistrado=1 / Serventuário=14)
#   movimento_codigo_k = id no nível k (k até 5)
get_hierarquia <- function(codigo_folha) {

  # Constrói a cadeia de baixo para cima
  cadeia <- list()
  id_atual <- as.integer(codigo_folha)

  for (nivel in 1:6) {  # máximo razoável de níveis
    no <- fetch_tpu_node(id_atual)
    if (is.null(no)) break

    cadeia <- c(cadeia, list(list(id = no$id, nome = no$nome)))

    pai <- no$cod_item_pai
    if (is.null(pai) || is.na(pai)) break  # chegou à raiz
    id_atual <- as.integer(pai)
  }

  # A cadeia está de folha→raiz; inverte para raiz→folha
  cadeia_ordenada <- rev(cadeia)
  n_niveis <- length(cadeia_ordenada)

  # Preenche até 5 níveis (NA se não existir)
  out <- tibble(
    movimento_codigo_1 = if (n_niveis >= 1) as.integer(cadeia_ordenada[[1]]$id) else NA_integer_,
    movimento_codigo_2 = if (n_niveis >= 2) as.integer(cadeia_ordenada[[2]]$id) else NA_integer_,
    movimento_codigo_3 = if (n_niveis >= 3) as.integer(cadeia_ordenada[[3]]$id) else NA_integer_,
    movimento_codigo_4 = if (n_niveis >= 4) as.integer(cadeia_ordenada[[4]]$id) else NA_integer_,
    movimento_codigo_5 = if (n_niveis >= 5) as.integer(cadeia_ordenada[[5]]$id) else NA_integer_,
    movimento_nome_1   = if (n_niveis >= 1) as.character(cadeia_ordenada[[1]]$nome) else NA_character_,
    movimento_nome_2   = if (n_niveis >= 2) as.character(cadeia_ordenada[[2]]$nome) else NA_character_,
    movimento_nome_3   = if (n_niveis >= 3) as.character(cadeia_ordenada[[3]]$nome) else NA_character_,
    movimento_nome_4   = if (n_niveis >= 4) as.character(cadeia_ordenada[[4]]$nome) else NA_character_,
    movimento_nome_5   = if (n_niveis >= 5) as.character(cadeia_ordenada[[5]]$nome) else NA_character_
  )
  out
}

# get_campos_folha(): retorna os campos TPU do nó folha (o movimento em si).
get_campos_folha <- function(codigo) {
  no <- fetch_tpu_node(as.integer(as.character(codigo)))
  if (is.null(no)) {
    return(tibble(
      tpu_id = NA_integer_, tpu_cod_item_pai = NA_integer_,
      tpu_nome = NA_character_, tpu_descricao_glossario = NA_character_,
      tpu_glossario = NA_character_, tpu_norma = NA_character_,
      tpu_artigo = NA_character_, tpu_dispositivo_legal = NA_character_,
      tpu_data_inclusao = NA_character_, tpu_data_versao = NA_character_,
      tpu_data_alteracao = NA_character_
    ))
  }
  tibble(
    tpu_id                 = as.integer(no$id),
    tpu_cod_item_pai       = as.integer(no$cod_item_pai %||% NA_integer_),
    tpu_nome               = as.character(no$nome %||% NA_character_),
    tpu_descricao_glossario = as.character(no$descricao_glossario %||% NA_character_),
    tpu_glossario          = as.character(no$glossario %||% NA_character_),
    tpu_norma              = as.character(no$norma %||% NA_character_),
    tpu_artigo             = as.character(no$artigo %||% NA_character_),
    tpu_dispositivo_legal  = as.character(no$dispositivoLegal %||% NA_character_),
    tpu_data_inclusao      = as.character(no$data_inclusao %||% NA_character_),
    tpu_data_versao        = as.character(no$data_versao %||% NA_character_),
    tpu_data_alteracao     = as.character(no$data_alteracao %||% NA_character_)
  )
}

## PROCESSAMENTO ---------------------------------------------------------------
log_info("PROC | Carregando df_movimentos")
df_mov <- readRDS(FILE_IN)
log_info("PROC | {nrow(df_mov)} linhas, {n_distinct(df_mov$movimento_codigo)} códigos únicos")

# Obtém os códigos únicos — o enriquecimento é por código, não por linha.
# Isso é fundamental: com N processos e M movimentos repetidos, evita N*M chamadas.
codigos_unicos <- df_mov |>
  filter(!is.na(movimento_codigo), as.character(movimento_codigo) != "{}") |>
  distinct(movimento_codigo) |>
  pull(movimento_codigo) |>
  as.character() |>
  as.integer()

log_info("PROC | {length(codigos_unicos)} códigos únicos para enriquecer via TPU")

# Processa cada código único
n_total  <- length(codigos_unicos)
n_feitos <- 0L

df_tpu_lookup <- map_dfr(codigos_unicos, function(cod) {
  n_feitos <<- n_feitos + 1L
  if (n_feitos %% 50 == 0)
    log_info("PROC | Progresso: {n_feitos}/{n_total} códigos processados")

  hier  <- get_hierarquia(cod)
  folha <- get_campos_folha(cod)

  bind_cols(tibble(movimento_codigo = cod), hier, folha)
})

# Salva cache atualizado em disco
saveRDS(tpu_cache, FILE_CACHE)
log_info("CACHE | Gravado com {length(tpu_cache)} entradas em {FILE_CACHE}")

## JOIN E PERSISTÊNCIA ---------------------------------------------------------
log_info("JOIN | Unindo df_movimentos com lookup TPU")

df_out <- df_mov |>
  mutate(movimento_codigo = suppressWarnings(as.integer(as.character(movimento_codigo)))) |>
  filter(!is.na(movimento_codigo)) |>
  left_join(df_tpu_lookup, by = "movimento_codigo")

log_info("JOIN | Dimensões finais: {nrow(df_out)} linhas x {ncol(df_out)} colunas")

saveRDS(df_out, FILE_OUT)
log_info("OUT  | Salvo: {FILE_OUT}")

## VALIDAÇÃO -------------------------------------------------------------------
n_magistrado   <- sum(df_out$movimento_codigo_1 == 1L,  na.rm = TRUE)
n_serventuario <- sum(df_out$movimento_codigo_1 == 14L, na.rm = TRUE)
n_na           <- sum(is.na(df_out$movimento_codigo_1))

log_info("VALID | Movimentos de Magistrado  : {n_magistrado}  ({round(n_magistrado/nrow(df_out)*100,1)}%)")
log_info("VALID | Movimentos de Serventuário: {n_serventuario}  ({round(n_serventuario/nrow(df_out)*100,1)}%)")
log_info("VALID | Sem classificação (NA)    : {n_na}")

## FINALIZAÇÃO -----------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- round(difftime(end_time, start_time, units = "secs"), 2)
log_info("Pipeline encerrado | tempo: {elapsed}s | fim: {format(end_time, '%Y-%m-%d %H:%M:%S')}")
rm(end_time, start_time, elapsed)
# eof --------------------------------------------------------------------------