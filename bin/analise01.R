#!/usr/bin/env Rscript

# SETUP -------------------------------------------------------------------

source("bin/datajud_load_n_clean.R")

start_time <- Sys.time()
message(sprintf("[INFO] Início do script em %s", format(start_time, "%Y-%m-%d %H:%M:%S")))

## pacotes ----
message("[INFO] Carregando pacotes (via pacman)...")
if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
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
  htmlwidgets
)

## funcoes ----
message("[INFO] Carregando funções auxiliares...")

### 3. Função para consultar a API do IBGE ----
get_municipio_ibge <- function(codigo) {
  
  url <- paste0(
    "https://servicodados.ibge.gov.br/api/v1/localidades/municipios/",
    codigo
  )
  
  resp <- try(GET(url), silent = TRUE)
  
  if (inherits(resp, "try-error") || status_code(resp) != 200) {
    return(tibble(
      orgao_julgador_ibge = codigo,
      municipio_nome = NA_character_,
      municipio_mesorregiao = NA_character_
    ))
  }
  
  conteudo <- content(resp, as = "text", encoding = "UTF-8")
  json <- fromJSON(conteudo, flatten = TRUE)
  
  tibble(
    orgao_julgador_ibge = codigo,
    municipio_nome = json$nome,
    municipio_mesorregiao = json$microrregiao.mesorregiao.nome
  )
}

message("[OK] Funções auxiliares carregadas.")

# CONEXÃO E CONSULTA -----------------------------------------------------
message("[INFO] Conectando ao DuckDB e carregando os dados...")

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

message(
  "[OK] Dados carregados. Tempo gasto: ", 
  round(
    difftime(
      Sys.time(), 
      start_time, 
      units = "secs"
    ), 
    2
  )
  , " segundos."
)


# DATA CHECK -------------------------------------------------------------------
## check loaded data ----
message("[INFO] Verificando os dataframes ingeridos ...")
glimpse(df_processos)
glimpse(df_movimentos)
glimpse(df_assuntos)

# DATA CLEAN -------------------------------------------------------------------
message("[OK] Dataframes verificados. Iniciando limpeza e análise exploratória ...")

## ajuste no df_processos ----
message("[INFO] Ajustando tipos (as factor) e informações temporais ...")
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
    data_ajuizamento = as.Date(data_ajuizamento),
    ano_ajuizamento = year(data_ajuizamento)
  )
message("[OK] Tipos ajustados. Explorando o dataframe de df_processos após ajuste...")
head(df_processos)
glimpse(df_processos)
summary(df_processos)

### busca informações do municipio na API do IBGE ----
message("[INFO] Buscando informações de município na API do IBGE ...")

#### 1. Garantir que o código IBGE é numérico ----
df_processos <- df_processos %>%
  mutate(orgao_julgador_ibge = as.integer(as.character(orgao_julgador_ibge)))

#### 2. Extrair códigos únicos (evita chamadas repetidas) ----
codigos_ibge <- df_processos %>%
  distinct(orgao_julgador_ibge) %>%
  filter(!is.na(orgao_julgador_ibge)) %>%
  pull(orgao_julgador_ibge)

#### 3. Consultar todos os municípios ----
tabela_municipios <- map_dfr(
  codigos_ibge, 
  get_municipio_ibge
)

#### 4. Fazer join com o dataframe original ----
df_processos <- df_processos %>%
  left_join(
    tabela_municipios, 
    by = "orgao_julgador_ibge"
  )

message("[OK] Informações de município adicionadas. Explorando o dataframe de df_processos após ajuste..")
head(df_processos)
glimpse(df_processos)
summary(df_processos)


## ajuste no df_movimentos ----
message("[INFO] Ajustando tipos (as factor) ...")
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
    movimento_data_hora = as.POSIXct(
      movimento_data_hora, 
      format = "%Y-%m-%dT%H:%M:%S", 
      tz = "UTC"
    ),
    movimento_mes = floor_date(movimento_data_hora, "month"),
    movimento_ano = year(movimento_data_hora)
  )
message("[OK] Tipos ajustados. Explorando o dataframe de df_movimentos após ajuste...")
head(df_movimentos)
glimpse(df_movimentos)
summary(df_movimentos)


# ajuste no df_assuntos
message("[INFO] Ajustando tipos (as factor)...")
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
message("[OK] Tipos ajustados. Explorando o dataframe de df_assuntos após ajuste...")
head(df_assuntos)
glimpse(df_assuntos)
summary(df_assuntos)



# subset com os movimentos de decisão de juizo de admissibilidade (430,433)
decisoes <- df_movimentos  |> 
  filter(
    movimento_codigo %in% c(430, 433)
  ) |>
  arrange(
    processo, 
    desc(movimento_data_hora)
  ) |>
  distinct(
    processo, 
    .keep_all = TRUE
  ) |>
  transmute(
    processo,
    status = if_else(
      movimento_codigo == 430, 
      "Admitido", 
      "Não admitido"
    ),
    data_decisao = movimento_data_hora
  ) |> 
  mutate(
    status = as.factor(status)
  )
head(decisoes)
glimpse(decisoes)
summary(decisoes)


ggplot(
  decisoes, 
  aes(x = data_decisao)) +
geom_histogram(bins = 100) +
labs(
  title = "Distribuição das decisões ao longo do tempo",
  x = "Data da decisão",
  y = "Frequência"
)

ggplot(
  decisoes, 
  aes(x = data_decisao)
) +
geom_density() +
labs(
    title = "Densidade temporal das decisões",
    x = "Data da decisão",
    y = "Densidade"
  )



serie_mes <- decisoes |>
  mutate(
    ano_mes = floor_date(
      data_decisao, 
      "month"
    )
  ) |>
  count(ano_mes)
glimpse(serie_mes)

ggplot(
  serie_mes, 
  aes(
    x = ano_mes, 
    y = n)
) +
geom_line() +
labs(
  title = "Número de decisões por mês",
  x = "Ano-mês",
  y = "Quantidade"
)

serie_mes_full <- serie_mes |>
  mutate(
    ano_mes = floor_date(ano_mes, "month")
  ) |>
  complete(
    ano_mes = seq(
      min(ano_mes),
      max(ano_mes),
      by = "month"
    ),
    fill = list(n = 0)   # ausência de decisão = 0
  ) |>
  arrange(ano_mes)




ts_decisoes <- ts(
  serie_mes_full$n,
  start = c(year(min(serie_mes_full$ano_mes)),
            month(min(serie_mes_full$ano_mes))),
  frequency = 12
)

decomp <- stl(
  ts_decisoes,
  s.window = "periodic"   # sazonalidade fixa anual
)

serie_mes_full$trend <- decomp$time.series[, "trend"]

plot(serie_mes_full$trend)


plot(decomp)

decomp_df <- tibble(
  data = serie_mes_full$ano_mes,
  observed = decomp$time.series[, "seasonal"] +
    decomp$time.series[, "trend"] +
    decomp$time.series[, "remainder"],
  trend = decomp$time.series[, "trend"],
  seasonal = decomp$time.series[, "seasonal"],
  remainder = decomp$time.series[, "remainder"]
) |>
  pivot_longer(-data, names_to = "componente", values_to = "valor")

ggplot(
  decomp_df, 
  aes(
    x = data, 
    y = valor)
) +
geom_line() +
facet_wrap(~ componente, ncol = 1, scales = "free_y") +
labs(
  title = "Juízo de Admissibilidade de REsp no TJPR",
  subtitle = "Decomposição STL da série mensal de decisões",
  x = "Data",
  y = NULL
  ) +
theme_minimal()

# Gráfico
ggsave(
  filename = "ts_decomposta_resp_tjpr_20260210.png",
  width = 12,      # polegadas
  height = 6.75,   # proporção 16:9
  dpi = 300,
  bg = "white"
)



# subset com o movimento de recebimento do REsp pela 1ªVice Presidencia
recebimentos <- df_movimentos |>
  filter(movimento_codigo == 132) |>
  dplyr::select(
    processo, 
    data_recebimento = movimento_data_hora
  )
head(recebimentos)
glimpse(recebimentos)


# une os dois subsets
dados_resp <- decisoes |>
  left_join(
    recebimentos, 
    by = "processo"
  ) |>
  filter(
    data_recebimento <= data_decisao
  ) |>
  arrange(
    processo, 
    desc(data_recebimento)
  ) |>
  distinct(
    processo, 
    .keep_all = TRUE
  )
head(dados_resp)
glimpse(dados_resp)


# ajuste temporal do subset 
dados_resp <- dados_resp |>
  mutate(
    tempo_dias = as.numeric(
      difftime(
        data_decisao, 
        data_recebimento, 
        units = "days"
      )
    ),
    ano_mes_decisao = floor_date(
      data_decisao, 
      "month"
    )
  )
head(dados_resp)
glimpse(dados_resp)

# elina observacoes com zero dias
dados_resp <- dados_resp |>
  filter(tempo_dias >= 0)
head(dados_resp)
glimpse(dados_resp)


# gera uma serie temporal
serie_tempo <- dados_resp |>
  group_by(ano_mes_decisao) |>
  summarise(
    n = n(),
    media_dias = mean(tempo_dias, na.rm = TRUE),
    mediana_dias = median(tempo_dias, na.rm = TRUE),
    p90 = quantile(tempo_dias, 0.90, na.rm = TRUE),
    .groups = "drop"
  )
print(serie_tempo, n = 50)


# gera uma serie temporal com o status de admissibilidade
serie_tempo_status <- dados_resp |>
  group_by(
    ano_mes_decisao, 
    status
  ) |>
  summarise(
    n = n(),
    media_dias = mean(tempo_dias, na.rm = TRUE),
    mediana_dias = median(tempo_dias, na.rm = TRUE),
    .groups = "drop"
  )
head(serie_tempo_status)


# ANALISE E GRÁFICOS -----------------------------------------------------------


## tempo entre o recebimento e a decisão de admissibilidade --------------------

# geral
ggplot(
  serie_tempo, 
  aes(
    x = ano_mes_decisao, 
    y = media_dias
  )
) +
geom_line() +
geom_smooth(
  se = FALSE, 
  method = "loess"
) +
labs(
  title = "Tempo entre recebimento e decisão de admissibilidade (TJPR)",
  x = "Data da decisão",
  y = "Dias (média)"
) +
theme_bw()

# Gráfico
ggsave(
  filename = "ts_geral_resp_tjpr_20260210.png",
  width = 12,      # polegadas
  height = 6.75,   # proporção 16:9
  dpi = 300,
  bg = "white"
)



# serie temporal por status

ggplot(
  serie_tempo_status,
  aes(
    x = ano_mes_decisao, 
    y = media_dias, 
    color = status
  )
) +
  # Série observada (pontilhada)
  geom_line(
    aes(linetype = status),
    linewidth = 0.5,
    alpha = 0.8
  ) +
  # Tendência LOESS (linha sólida)
  geom_smooth(
    se = FALSE,
    method = "loess",
    linewidth = 1.2
  ) +
  # Cores definidas manualmente
  scale_color_manual(
    values = c(
      "Admitido" = "#2C7FB8",      # azul
      "Não admitido" = "#D7301F"   # vermelho
    )
  ) +
  # Tipo de linha: dados pontilhados
  scale_linetype_manual(
    values = c(
      "Admitido" = "dotted",
      "Não admitido" = "dotted"
    )
  ) +
  labs(
    title = "Juízo de Admissibilidade de REsp no TJPR",
    subtitle = "Quanto tempo a 1ª Vice Presidência leva para realizar o juízo de admissibilidade",
    x = "Data da decisão",
    y = "Dias (média)",
    color = "Resultado",
    linetype = "Resultado",
    caption = "Fonte: DataJud/CNJ. Elaboração própria. Nota: tempo entre o recebimento do REsp (mov. 132) e a decisão de admissibilidade (movs. 430 e 433)."
  ) +
  
  ggthemes::theme_gdocs() +
  theme(
    plot.caption = element_text(
      size = 8,
      hjust = 0,
      margin = margin(t = 8)
    ),
    plot.title = element_text(face = "bold"),
    legend.position = "bottom"
  )

ggsave(
  filename = "ts_tempo_tjpr_20260210.png",
  width = 12,
  height = 6.75,
  dpi = 300,
  bg = "white"
)




## analise a tendencia temporal de status --------------------------------------

# subset de 2018 até hoje
decisoes2018 <- decisoes |> 
  filter(data_decisao >= as.Date("2018-01-01"))

# proporcao mensal de REsp admitidos
prop_mensal <- decisoes2018 |>
  mutate(
    mes = floor_date(data_decisao, "month")
  ) |>
  count(
    mes, 
    status
  ) |>
  group_by(mes) |>
  mutate(
    total = sum(n), 
    prop = n / total
  ) |>
  ungroup() |>
  filter(status == "Admitido")



# Analise por regressão logística ----------------------------------------------
# modelar a probabilidade de um REsp ser admitido ao longo do tempo
# (tendência temporal)


decisoes_bin <- decisoes2018 |>
  mutate(
    # Variável dependente binária:
    # 1 = decisão "Admitido"
    # 0 = qualquer outro resultado
    # Conversão para inteiro é necessária para o glm binomial
    y = as.integer(status == "Admitido"),
    
    # Variável explicativa de tempo contínuo (em anos)
    # Calcula a diferença entre a data da decisão e 01/01/2018 (inicio serie)
    # em dias e converte para anos (365,25 considera anos bissextos)
    t = as.numeric(
      difftime(
        data_decisao, 
        as.Date("2018-01-01"), 
        units = "days"
      )
    ) / 365.25
  )


# Estimação do modelo
# Modelo: Regressão logística (logit)
# logit(P(admitido)) = β0 + β1 * t
#
# Interpretação:
# β1 captura a tendência temporal na probabilidade de admissão.


fit <- glm(
  y ~ t, 
  data = decisoes_bin, 
  family = binomial
)

# Sumário do modelo:
# - Coeficientes (em log-odds)
# - Testes de significância
# - Qualidade do ajuste (deviance, AIC)
summary(fit)
stargazer::stargazer(
  fit,
  type = "text"
)


# Interpretação econômica/estatística
# exp(beta_t) = Odds Ratio (OR) associado a um aumento de 1 ano
# OR > 1  → probabilidade de admissão está aumentando ao longo do tempo
# OR < 1  → probabilidade de admissão está diminuindo ao longo do tempo
exp(coef(fit)["t"])

# Intervalo de confiança para o OR
# Utiliza o método profile likelihood (mais robusto que Wald)
exp(confint(fit, "t"))



# Analise com modelo logístico com tendência temporal flexível -----------------
# permitir que o efeito do tempo sobre a probabilidade
# de admissão varie de forma não linear.
#
# ns() = natural cubic splines (splines cúbicos naturais)
# df = 5 define o grau de flexibilidade (número de parâmetros efetivos)
#
# Quanto maior o df:
#  - maior a flexibilidade
#  - maior o risco de overfitting
# Este modelo relaxa a hipótese de tendência linear imposta no modelo anterior.

fit_ns <- glm(
  y ~ ns(
    t, 
    df = 5
  ), 
  data = decisoes_bin, 
  family = binomial
)
summary(fit_ns)
stargazer::stargazer(
  fit_ns,
  type = "text"
)

# Comparação de modelos
# AIC (Akaike Information Criterion)
# Critério de seleção que penaliza complexidade:
# AIC = -2 log-likelihood + 2k
#
# Interpretação:
# - Menor AIC → melhor equilíbrio entre ajuste e parcimônia
# - Se AIC(fit_ns) << AIC(fit), há evidência de não linearidade no tempo

AIC(
  fit,     # modelo linear em t
  fit_ns   # modelo com splines
)


# Predição para construção de curva suave de probabilidade

pred_df <- tibble(
  
  # Grade temporal contínua (em anos desde o inicio)
  # length.out = 200 gera uma curva suave para visualização
  t = seq(
    0, 
    max(decisoes_bin$t), 
    length.out = 200
  )
  
) |>
  mutate(
    
    # Probabilidade prevista pelo modelo spline
    # type = "response" retorna probabilidades (escala original),
    # não log-odds
    prob = predict(
      fit_ns, 
      newdata = pick(everything()), 
      type = "response"
    ),
    
    # Conversão da variável t (anos desde 2015)
    # de volta para uma data de calendário
    # útil para plotagem em eixo temporal real
    data = as.Date("2018-01-01") + t * 365.25
  )



ggplot(
  prop_mensal, 
  aes(
    mes, 
    prop
  )
) +
geom_point(
  aes(
    size = total
  ), 
  alpha = 0.4
) +
geom_line(
  data = pred_df, 
  aes(data, prob), 
  color = "red", 
  linewidth = 1
) +
scale_y_continuous(
  labels = scales::percent,
  limits = c(0, NA)
) +
labs(
  x = "Mês", 
  y = "Taxa de Admissão",
  title = "Juízo de Admissibilidade de REsp no TJPR",
  subtitle= "Probabilidade de admissão do REsp"
) +
theme_minimal()


ggsave(
  filename = "logit_probabilidade_admissao_tjpr_20260210.png",
  width = 12,
  height = 6.75,
  dpi = 300,
  bg = "white"
)





fit_seg <- segmented(
  fit, 
  seg.Z = ~t, 
  npsi = 1
)
summary(fit_seg)
#### nota: o breakpoint estimado te dá o momento da mudança estrutural

ggplot(
  prop_mensal, 
  aes(
    mes, 
    prop)
) +
geom_point(
  aes(size = total), 
  alpha = 0.4
) +
geom_line(alpha = 0.3) +
geom_smooth(
  method = "loess", 
  span = 0.3, 
  aes(weight = total), 
  se = TRUE
) +
scale_y_continuous(
  labels = scales::percent, 
  limits = c(0, NA)
) +
labs(
  x = "Mês", y = "Taxa de Admissão",
  title = "Evolução da taxa de admissão (2015+)",
  size = "n/mês"
) +
theme_minimal()




#eof----------------------------------------------------------------------------