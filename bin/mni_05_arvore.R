#!/usr/bin/env Rscript
# bin/mni_05_arvore_processual.R
# Extrai e aplana a árvore processual (ARVORE_PROCESSUAL) dos JSONs organizados do MNI.
# Input : data_mni/processed/*.json  (JSONs organizados pelo mni_01_getdata_batch.py)
# Output: data_mni/arvore_mni.parquet
# Uso   : Rscript bin/mni_05_arvore_processual.R

# ── MODELO DE DADOS ────────────────────────────────────────────────────────────
#
# A árvore é um DAG enraizado: processo de 1º grau (G1) → recursos (G2).
# Representação tabular: lista de adjacência de nós (uma linha por nó).
#
# Schema — arvore_mni:
#   processo_raiz        VARCHAR   numeroUnico do nó raiz (G1) — chave de agrupamento
#   numero_processo      VARCHAR   numeroUnico do nó atual
#   numero_pai           VARCHAR   numeroUnico do pai; NA para a raiz
#   profundidade         INTEGER   0 = raiz G1; 1 = recurso direto; 2 = incidente; ...
#   ordem_entre_irmaos   INTEGER   posição 1-indexed no array processos[] do pai
#   instancia            VARCHAR   "1" (G1) ou "2" (G2) conforme JSON
#   grau                 VARCHAR   "G1" | "G2"  (taxonomia DATAJUD)
#   sequencial           INTEGER   campo sequencial do JSON (−1 raiz, 0 demais)
#   eh_raiz              LOGICAL   TRUE apenas para o nó raiz do processo
#   eh_folha             LOGICAL   TRUE se processos[] é vazio (recurso terminal)
#
# Joins habilitados após geração:
#   arvore_mni.numero_processo → processos_mni.numero_processo  (classe, assunto, valor)
#   arvore_mni.processo_raiz   → partes_mni.numero_processo     (habilitações de crédito)
#   arvore_mni.numero_processo → movimentos_mni.numero_processo  (acórdãos, decisões)
#
# Análises downstream (fora do escopo deste script):
#   - Habilitações por tipo de feito (falência / RJ) via classe_processual
#   - Litigiosidade: COUNT(*) WHERE profundidade = 1 GROUP BY processo_raiz
#   - Profundidade máxima da cadeia recursal: MAX(profundidade) GROUP BY processo_raiz
#   - Estabilidade decisória: join com movimentos_mni filtrando códigos CNJ de acórdão
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
  paste0("mni_arvore_processual_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

log_threshold(DEBUG)
log_appender(appender_console,        index = 1)
log_layout(layout_glue_colors,        index = 1)
log_appender(appender_file(log_file), index = 2)
log_layout(layout_glue,               index = 2)

log_info("Pipeline iniciado")

## BANNER ----------------------------------------------------------------------
cat("-----------------------------------------------------------------------------------\n")
cat("-------- MNI: ÁRVORE PROCESSUAL → arvore_mni.parquet ----------------------------\n")
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

## SETUP -----------------------------------------------------------------------
log_info("SETUP | Carregando pacotes")

pacman::p_load(jsonlite, purrr, dplyr, tibble, arrow, readr)

log_info("SETUP | Pacotes carregados")

script_path <- normalizePath(
  sub("--file=", "", grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)),
  winslash = "/"
)
PROJ_ROOT <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/")
DIR_DATA  <- file.path(PROJ_ROOT, "data_mni")
DIR_JSON  <- file.path(DIR_DATA, "processed")
FILE_OUT  <- file.path(DIR_DATA, "arvore_mni.parquet")

if (!dir.exists(DIR_JSON)) {
  log_error("SETUP | Diretório de JSONs não encontrado: {DIR_JSON}")
  stop("Diretório não encontrado: ", DIR_JSON)
}

log_info("SETUP | DIR_JSON : {DIR_JSON}")
log_info("SETUP | FILE_OUT : {FILE_OUT}")

## FUNÇÃO RECURSIVA DE APLANAMENTO --------------------------------------------
#
# flatten_node() desce recursivamente na árvore e produz uma linha por nó.
# Argumentos:
#   node          — lista R com campos numeroUnico, sequencial, instancia, processos
#   processo_raiz — numeroUnico da raiz do processo (propagado em toda a recursão)
#   numero_pai    — numeroUnico do pai; NA_character_ para a raiz
#   profundidade  — inteiro, 0 para raiz
#   ordem         — posição 1-indexed entre os irmãos; 1 para a raiz (sem irmãos)
#
flatten_node <- function(node, processo_raiz, numero_pai, profundidade, ordem) {

  filhos   <- node$processos
  eh_folha <- length(filhos) == 0
  instancia <- as.character(node$instancia)

  linha <- tibble(
    processo_raiz      = processo_raiz,
    numero_processo    = as.character(node$numeroUnico),
    numero_pai         = numero_pai,
    profundidade       = profundidade,
    ordem_entre_irmaos = ordem,
    instancia          = instancia,
    grau               = if (instancia == "1") "G1" else "G2",
    sequencial         = as.integer(node$sequencial),
    eh_raiz            = is.na(numero_pai),
    eh_folha           = eh_folha
  )

  if (eh_folha) return(linha)

  # Desce para os filhos, passando a ordem posicional (1-indexed)
  linhas_filhos <- imap_dfr(filhos, function(filho, idx) {
    flatten_node(
      node          = filho,
      processo_raiz = processo_raiz,
      numero_pai    = as.character(node$numeroUnico),
      profundidade  = profundidade + 1L,
      ordem         = idx
    )
  })

  bind_rows(linha, linhas_filhos)
}

## PARSER POR ARQUIVO ----------------------------------------------------------
#
# parse_arvore() lê um JSON organizado, localiza ARVORE_PROCESSUAL dentro de
# processo.dados_basicos.outrosParametros e chama flatten_node() na raiz.
#
# A estrutura é:
#   d$processo$dados_basicos$outrosParametros  — lista de listas com $nome e $valor_estruturado
#   O item com $nome == "ARVORE_PROCESSUAL" contém $valor_estruturado$arvoreProcessual
#   que é uma lista de comprimento 1 cujo único elemento é o nó raiz.
#
parse_arvore <- function(path) {
  tryCatch({
    d <- fromJSON(path, simplifyVector = FALSE)

    outros <- d$processo$dados_basicos$outrosParametros

    # Localiza o item ARVORE_PROCESSUAL — não assume posição fixa
    idx_arvore <- detect_index(outros, ~ .x$nome == "ARVORE_PROCESSUAL")

    if (idx_arvore == 0L) {
      log_warn("PARSE | Sem ARVORE_PROCESSUAL: {basename(path)}")
      return(NULL)
    }

    arvore_list <- outros[[idx_arvore]]$valor_estruturado$arvoreProcessual

    if (length(arvore_list) == 0) {
      log_warn("PARSE | arvoreProcessual vazia: {basename(path)}")
      return(NULL)
    }

    # O primeiro (e único) elemento é a raiz G1
    raiz <- arvore_list[[1]]
    processo_raiz <- as.character(raiz$numeroUnico)

    flatten_node(
      node          = raiz,
      processo_raiz = processo_raiz,
      numero_pai    = NA_character_,
      profundidade  = 0L,
      ordem         = 1L
    )

  }, error = function(e) {
    log_error("PARSE | ERRO em {basename(path)}: {conditionMessage(e)}")
    NULL
  })
}

## PROCESSAMENTO ---------------------------------------------------------------
arquivos <- list.files(DIR_JSON, pattern = "\\.json$", full.names = TRUE)
log_info("PROC | {length(arquivos)} JSONs encontrados em {DIR_JSON}")

if (length(arquivos) == 0) {
  log_error("PROC | Nenhum JSON encontrado. Verifique DIR_JSON.")
  stop("Nenhum arquivo .json em: ", DIR_JSON)
}

df_arvore <- map(arquivos, parse_arvore) |>
  compact()                               |>
  bind_rows()

n_processos <- n_distinct(df_arvore$processo_raiz)
n_nos       <- nrow(df_arvore)
n_recursos  <- sum(df_arvore$profundidade == 1L, na.rm = TRUE)
prof_max    <- max(df_arvore$profundidade, na.rm = TRUE)

log_info("PROC | Parsing concluído")
log_info("PROC | Processos raiz          : {n_processos}")
log_info("PROC | Total de nós (linhas)   : {n_nos}")
log_info("PROC | Recursos diretos (G2 d1): {n_recursos}")
log_info("PROC | Profundidade máxima     : {prof_max}")

## PERSISTÊNCIA ----------------------------------------------------------------
write_parquet(df_arvore, FILE_OUT, compression = "zstd")
log_info("OUT  | Parquet gravado: {FILE_OUT}")

write_csv(df_arvore, sub("\\.parquet$", ".csv", FILE_OUT))
log_info("OUT  | CSV gravado   : {sub('\\.parquet$', '.csv', FILE_OUT)}")

## VALIDAÇÃO -------------------------------------------------------------------
log_info("VALID | Schema:")
for (nm in names(df_arvore)) {
  log_info("VALID |   {nm} [{class(df_arvore[[nm]])[1]}]")
}

# Distribuição por profundidade
dist_prof <- df_arvore |>
  count(profundidade, grau) |>
  arrange(profundidade)

log_info("VALID | Distribuição por profundidade:")
walk(seq_len(nrow(dist_prof)), function(i) {
  log_info("VALID |   profundidade={dist_prof$profundidade[i]}  grau={dist_prof$grau[i]}  n={dist_prof$n[i]}")
})

# Processos sem nenhum recurso (árvore com apenas a raiz)
sem_recurso <- df_arvore |>
  group_by(processo_raiz) |>
  summarise(max_prof = max(profundidade)) |>
  filter(max_prof == 0) |>
  nrow()

log_info("VALID | Processos sem recursos (árvore trivial): {sem_recurso}")

## FINALIZAÇÃO -----------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- round(difftime(end_time, start_time, units = "secs"), 2)
log_info("Pipeline encerrado com sucesso | tempo: {elapsed}s | fim: {format(end_time, '%Y-%m-%d %H:%M:%S')}")
rm(end_time, start_time, elapsed)
# eof --------------------------------------------------------------------------