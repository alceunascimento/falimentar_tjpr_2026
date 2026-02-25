#!/usr/bin/env Rscript
# datajud_obter_daados_classes_codigo.R
# Objetivo: coletar dados processuais do DataJud por codigo de classe.
# Run: `datajud_getdata_assuunto.R {tribunal} {codigo} {codigo} {codigo} ....`
# Estrutura de pastas esperada ():
#   ├── data/   → saída de arquivos .rds
#   ├── logs/   → arquivos de log

# SETUP -------------------------------------------------------------------------
start_time <- Sys.time()
message(sprintf("[INFO] Início do script em %s", format(start_time, "%Y-%m-%d %H:%M:%S")))

## 0) Pacotes -------------------------------------------------------------------
message("[INFO] Carregando pacotes...")

## set CRAN packages ----
cran_pkgs <- c(
  "httr",        # Realiza requisições HTTP, útil para interagir com APIs REST
  "usethis",     # Automate Package and Project Setup
  "tidyverse",   # Conjunto integrado de pacotes para manipulação, transformação e visualização de dados
  "glue",        # Facilita a interpolação de strings de forma eficiente e legível
  "janitor",     # Funções para limpeza e organização de dados, incluindo padronização de nomes de colunas
  "jsonlite",    # Manipulação de dados no formato JSON (leitura e escrita)
  "tictoc",      # Mede o tempo de execução de blocos de código
  "kableExtra",  # Criação e formatação avançada de tabelas HTML/LaTeX
  "lubridate",   # Manipulação e formatação de datas e horas de forma simples
  "DBI",         # Interface genérica para bancos de dados em R
  "RSQLite",     # Driver para trabalhar com bancos de dados SQLite
  "parallel"     # Para processamento paralelo
)

# install pacman
# Ensure pacman exists (only installs if missing)
if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman", repos = "https://cran-r.c3sl.ufpr.br/")
}
# Load/install all of them
pacman::p_load(cran_pkgs, character.only = TRUE)

message("[OK] Pacotes carregados...")

## 1) Caminhos ---------------------------------------------------------------
script_dir <- normalizePath(getwd())             
data_dir   <- file.path(script_dir, "data")      
logs_dir   <- file.path(script_dir, "logs")      

# Criar diretórios se não existirem
if (!dir.exists(data_dir)) {
  dir.create(data_dir, recursive = TRUE)
  message(sprintf("[INFO] Diretório criado: %s", data_dir))
}
if (!dir.exists(logs_dir)) {
  dir.create(logs_dir, recursive = TRUE)
  message(sprintf("[INFO] Diretório criado: %s", logs_dir))
}

## 2) Carregar funções auxiliares -------------------------------------------
### Define a API ----
header <- c(
  "Authorization" = "ApiKey cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw==",
  "Content-Type" = "application/json"
)
message(sprintf("[INFO] CNJ Datajud API set: %s",
                header[1]
)
)

### Define a função de pesquisa do tribunal ----
pesquisar_tribunal <- function(tribunal, body, max_pag = 20) {
  usethis::ui_info("Pesquisando tribunal {tribunal}...")
  endpoint <- glue::glue(
    "https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search"
  )
  pag <- 1
  usethis::ui_info("Pesquisando página {pag}...")
  
  r <- httr::POST(
    endpoint,
    body = body,
    encode = "json",
    httr::add_headers(header)
  )
  r_list <- httr::content(r)$hits$hits
  
  len_list <- length(r_list)
  usethis::ui_info("Foram encontrados {len_list} registros.")
  
  while (len_list == 10000 && pag < max_pag) {
    pag <- pag + 1
    usethis::ui_info("Pesquisando página {pag}...")
    last_sort <- r_list |>
      dplyr::last() |>
      purrr::pluck("sort") |>
      unlist()
    body$search_after <- list(last_sort)
    r <- httr::POST(
      endpoint,
      body = body,
      encode = "json",
      httr::add_headers(header)
    )
    r_pag <- httr::content(r)$hits$hits  # isso aqui é o que dá problema !!!!
    r_list <- c(r_list, r_pag)
    
    len_list <- length(r_pag)
    usethis::ui_info("Foram encontrados {len_list} registros.")
  }
  r_list
}

### Define a função body de query ----
body_query_classe_codigo <- function(classe_codigo, size = 10000, order = "asc") {
  body <- list(
    query = list(
      bool = list(
        must = list(
          match = list(
            "classe.codigo" = classe_codigo
          )
        )
      )
    ),
    size = size,
    sort = list(
      list(
        "@timestamp" = list(
          order = order
        )
      )
    )
  )
  return(body)
}

# ARGUMENTOS DA LINHA DE COMANDO ----------------------------------------
args <- commandArgs(trailingOnly = TRUE)

# Validação dos argumentos
if (length(args) < 2) {
  stop(
    "[ERRO] Uso incorreto. Sintaxe esperada:\n",
    "  Rscript datajud_getdata_classe.R {tribunal} {classe_codigo} [{classe_codigo} ...]\n",
    "Exemplos:\n",
    "  Rscript datajud_getdata_classe.R tjpr 7\n",
    "  Rscript datajud_getdata_classe.R tjsp 7 94\n",
    call. = FALSE
  )
}

tribunal <- args[1]
classe_codigos <- as.integer(args[-1])

message(sprintf("[INFO] Tribunal: %s", tribunal))
message(sprintf("[INFO] Códigos de classe: %s", paste(classe_codigos, collapse = ", ")))


# OBTER DADOS DO DATAJUD -------------------------------------------
message("[INFO] Acessando o Datajud...")

n_cores <- min(length(classe_codigos), detectCores() - 1L)
message(sprintf("[INFO] Usando %d workers para %d classes.", n_cores, length(classe_codigos)))

buscar_classe <- function(classe_codigo) {
  message(sprintf("[INFO][PID %d] Iniciando classe %s ...", Sys.getpid(), classe_codigo))
  
  query <- body_query_classe_codigo(classe_codigo = classe_codigo)
  resultado_raw <- pesquisar_tribunal(tribunal, query)
  
  timestamp <- format(Sys.time(), "%Y%m%d%H%M%S")
  nome_arquivo <- glue("datajud_raw_{classe_codigo}_{timestamp}.ndjson")
  path_arquivo <- file.path(data_dir, nome_arquivo)
  
  sources <- lapply(resultado_raw, function(x) x[["_source"]])
  writeLines(sapply(sources, jsonlite::toJSON, auto_unbox = TRUE), path_arquivo)
  
  message(sprintf("[OK][PID %d] Classe %s salva em: %s", Sys.getpid(), classe_codigo, path_arquivo))
  return(invisible(path_arquivo))
}

resultados <- parallel::mclapply(
  X        = classe_codigos,
  FUN      = buscar_classe,
  mc.cores = n_cores
)

# END ---------------------------------------------------------------------
end_time <- Sys.time()
message(sprintf(
  "[INFO] Script finalizado sem erros. Duração: %0.2f minutos",
  as.numeric(difftime(end_time, start_time, units = "mins"))
))
#eof---------------------------------------------------------------------