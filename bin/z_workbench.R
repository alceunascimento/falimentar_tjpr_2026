#!/usr/bin/env Rscript

rm(list = ls())
gc()

#source("bin/datajud_load_n_clean.R")

source("bin/datajud_load_n_clean_logger.R")

if (!requireNamespace("pacman", quietly = TRUE)) install.packages("pacman")
pacman::p_load(
  httr,
  tidyverse,
  jsonlite,
  lubridate,
  splines,
  stargazer,
  segmented,
  duckdb,
  plotly,
  htmlwidgets
)

#-------------------------------------------------------------------------------

varas_especializadas <- c(26404, 26422, 13951, 13952, 13950, 13752, 8816, 8809)

tabela_distribuicao <- df_processos |>
  filter(mes_ajuizamento >= "2015-01-01") |>
  mutate(
    grupo = if_else(
      as.integer(as.character(orgao_julgador_codigo)) %in% varas_especializadas,
      "especializada",
      "nao_especializada"
    ),
    orgao_label = if_else(
      grupo == "especializada",
      as.character(orgao_julgador_nome),
      "nao_especializada (agrupadas)"
    )
  ) |>
  count(grupo, orgao_label, mes_ajuizamento) |>
  complete(
    nesting(grupo, orgao_label),
    mes_ajuizamento = seq(
      as.Date("2015-01-01"),
      as.Date("2025-10-01"),
      by = "month"
    ),
    fill = list(n = 0)
  ) |>
  pivot_wider(
    names_from  = mes_ajuizamento,
    values_from = n,
    values_fill = 0
  ) |>
  arrange(grupo, orgao_label)
tabela_distribuicao

write_csv(tabela_distribuicao,"procesos_distribuidos_mes_2015_2025.csv")


tabela_long <- tabela_distribuicao |>
  pivot_longer(
    cols      = -c(grupo, orgao_label),
    names_to  = "mes",
    values_to = "n"
  ) |>
  mutate(mes = as.Date(mes)) |>
  filter(mes >= as.Date("2015-01-01"), mes <= as.Date("2025-10-01")) |>
  group_by(grupo, mes) |>
  summarise(n = sum(n), .groups = "drop")

p1 <- plot_ly(
  data   = tabela_long,
  x      = ~mes,
  y      = ~n,
  color  = ~grupo,
  type   = "scatter",
  mode   = "lines",
  colors = c("especializada" = "#1f77b4", "nao_especializada" = "#ff7f0e")
) |>
  layout(
    title  = "Distribuição Mensal de Processos — Especializadas vs. Não Especializadas",
    xaxis  = list(title = "Mês de Ajuizamento", tickformat = "%Y-%m"),
    yaxis  = list(title = "Quantidade de Processos"),
    legend = list(title = list(text = "Grupo")),
    hovermode = "x unified"
  )
p1

htmlwidgets::saveWidget(
  widget   = p1,
  file     = here::here("output", "p1_distribuicao_mensal.html"),
  selfcontained = TRUE
)


tabela_long2 <- tabela_distribuicao |>
  pivot_longer(
    cols      = -c(grupo, orgao_label),
    names_to  = "mes",
    values_to = "n"
  ) |>
  mutate(mes = as.Date(mes)) |>
  filter(mes >= as.Date("2015-01-01"), mes <= as.Date("2025-10-01")) |>
  group_by(orgao_label, mes) |>        # <- colapsa duplicatas de orgao_label
  summarise(n = sum(n), .groups = "drop")


p2 <- plot_ly(
  data   = tabela_long2,
  x      = ~mes,
  y      = ~n,
  color  = ~orgao_label,
  type   = "scatter",
  mode   = "lines"
) |>
  layout(
    title  = "Distribuição Mensal de Processos — Especializadas vs. Não Especializadas",
    xaxis  = list(title = "Mês de Ajuizamento", tickformat = "%Y-%m"),
    yaxis  = list(title = "Quantidade de Processos"),
    legend = list(title = list(text = "Vara")),
    hovermode = "x unified"
  )
p2

htmlwidgets::saveWidget(
  widget        = p2,
  file          = here::here("output", "p2_distribuicao_mensal.html"),
  selfcontained = TRUE
)



tabela_nao_especializadas <- df_processos |>
  filter(
    !as.integer(as.character(orgao_julgador_codigo)) %in% varas_especializadas,
    mes_ajuizamento >= as.Date("2024-01-01")
  ) |>
  count(orgao_julgador_nome, mes_ajuizamento) |>
  pivot_wider(
    names_from  = mes_ajuizamento,
    values_from = n,
    values_fill = 0
  ) |>
  arrange(orgao_julgador_nome)

write_csv(tabela_nao_especializadas,"distribuicao_mensal_pos_especializacao_varas_nao_especializadas.csv")

tabela_nao_especializadas |>
  knitr::kable(format = "html") |>
  kableExtra::kable_classic(
    lightable_options = "basic",
    html_font         = '"Arial Narrow", "Source Sans Pro", sans-serif'
  )

DT::datatable(
  tabela_nao_especializadas,
  extensions = "Buttons",
  options = list(
    dom        = "Bfrtip",
    buttons    = c("copy", "csv", "excel"),
    pageLength = 20,
    scrollX    = TRUE
  ),
  rownames = FALSE,
  filter   = "top"
) |>
  htmlwidgets::saveWidget(
    file          = here::here("output", "tabela_nao_especializadas.html"),
    selfcontained = TRUE
  )






lista_nao_especializadas <- df_processos |>
  filter(
    !as.integer(as.character(orgao_julgador_codigo)) %in% varas_especializadas,
    mes_ajuizamento >= as.Date("2024-01-01")
  ) |>
  arrange(orgao_julgador_nome, data_ajuizamento)
lista_nao_especializadas

processos_incompetencia <- df_movimentos |>
  filter(movimento_codigo == 941) |>
  select(processo, data_movimento_incompetencia = movimento_data_hora) |>
  inner_join(df_processos, by = "processo", relationship = "many-to-many") |>
  filter(
    mes_ajuizamento >= "2024-01-01"
  ) |> 
  arrange(
    processo, 
    data_movimento_incompetencia
  ) |> 
  select(
    -id,
    -orgao_julgador_municipio_ibge,
    -tribunal,
    -grau,
    -data_ajuizamento_raw
  )
glimpse(processos_incompetencia)


write_csv(processos_incompetencia, "processos_com_decisao_incompetencia_pos_especializacao.csv")

tabela_incompetencia_temporal <- processos_incompetencia |>
  mutate(mes = floor_date(as.Date(data_movimento_incompetencia), "month")) |>
  count(municipio_nome, mes)

p3 <- plot_ly(
  data  = tabela_incompetencia_temporal,
  x     = ~mes,
  y     = ~n,
  color = ~municipio_nome,
  type  = "scatter",
  mode  = "lines+markers"
) |>
  layout(
    title     = "Decisões de Incompetência por Município",
    xaxis     = list(title = "Mês", tickformat = "%Y-%m"),
    yaxis     = list(title = "Quantidade de Decisões"),
    legend    = list(title = list(text = "Município")),
    hovermode = "x unified"
  )
p3

htmlwidgets::saveWidget(
  widget        = p3,
  file          = here::here("output", "p3_distribuicao_decisao_incompetencia.html"),
  selfcontained = TRUE
)



# incluir status de arquivado
processos_arquivados <- df_movimentos |>
  filter(movimento_codigo == 246) |>
  distinct(processo) |>
  mutate(status_arquivamento_definitivo = TRUE)
processos_arquivados

df_processos <- df_processos |>
  left_join(processos_arquivados, by = "processo") |>
  mutate(
    status_arquivamento_definitivo = replace_na(status_arquivamento_definitivo, FALSE)
  )
df_processos |> count(status_arquivamento_definitivo)



arquivados_nao_especializadas <- df_processos |>
  filter(!as.integer(as.character(orgao_julgador_codigo)) %in% varas_especializadas) |>
  group_by(orgao_julgador_nome) |>
  summarise(
    quantidade_processos_ajuizados             = n(),
    quantidade_processos_arquivamento_definitivo = sum(status_arquivamento_definitivo),
    quantidade_processos_ativos                = sum(!status_arquivamento_definitivo),
    .groups = "drop"
  ) |>
  arrange(orgao_julgador_nome)
arquivados_nao_especializadas

write_csv(arquivados_nao_especializadas, "processos_arquivados_nao_especializadas.csv")




ajuizamentos_mes <- df_processos |>
  filter(!as.integer(as.character(orgao_julgador_codigo)) %in% varas_especializadas) |>
  count(orgao_julgador_codigo, orgao_julgador_nome, mes = mes_ajuizamento) |>
  mutate(n = n)

arquivamentos_mes <- df_movimentos |>
  filter(movimento_codigo == 246) |>
  inner_join(
    df_processos |>
      filter(!as.integer(as.character(orgao_julgador_codigo)) %in% varas_especializadas) |>
      select(processo, orgao_julgador_codigo, orgao_julgador_nome),
    by = "processo"
  ) |>
  mutate(mes = floor_date(movimento_data_hora, "month")) |>
  count(orgao_julgador_codigo, orgao_julgador_nome, mes) |>
  mutate(n = -n)

tabela_fluxo_unico <- bind_rows(
  ajuizamentos_mes  |> mutate(tipo = "ajuizamento"),
  arquivamentos_mes |> mutate(tipo = "arquivamento")
) |>
  filter(mes >= as.Date("2015-01-01"), mes <= as.Date("2025-10-01"))


ggplot(tabela_fluxo_unico, aes(x = as.Date(mes), y = n, color = as.factor(orgao_julgador_codigo), linetype = tipo)) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey40") +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(
    title = "Fluxo Mensal — Varas Não Especializadas",
    x     = "Mês",
    y     = "Quantidade (ajuizamento +, arquivamento −)"
  ) +
  theme_minimal() +
  theme(
    legend.position = "none",
    axis.text.x     = element_text(angle = 45, hjust = 1)
  )


class(tabela_fluxo_unico$mes)
range(tabela_fluxo_unico$mes)
nrow(tabela_fluxo_unico)
tabela_fluxo_unico |> count(tipo)
tabela_fluxo_unico |> summarise(min = min(mes), max = max(mes))

#-------------------------------------------------------------------------------

transito <- df_movimentos |>
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
    transito_status = if_else(
      movimento_codigo == 848, 
      "sem_transito", 
      "com_transito"
    ),
    transito_data = movimento_data_hora
  )



decisoes <- df_movimentos |>
  filter(movimento_codigo %in% c(430, 433)) |>
  arrange(processo, desc(movimento_data_hora)) |>
  distinct(processo, .keep_all = TRUE) |>
  transmute(
    processo,
    status = if_else(movimento_codigo == 430, "Admitido", "Não admitido"),
    data_decisao = movimento_data_hora
  )

recebimentos <- df_movimentos |>
  filter(movimento_codigo == 132) |>
  select(processo, data_recebimento = movimento_data_hora)

dados_resp <- decisoes |>
  left_join(recebimentos, by = "processo") |>
  filter(data_recebimento <= data_decisao) |>
  arrange(processo, desc(data_recebimento)) |>
  distinct(processo, .keep_all = TRUE)

dados_resp <- dados_resp |>
  mutate(
    tempo_dias = as.numeric(difftime(data_decisao, data_recebimento, units = "days")),
    ano_mes_decisao = floor_date(data_decisao, "month")
  )

dados_resp <- dados_resp |>
  filter(tempo_dias >= 0)


serie_tempo <- dados_resp |>
  group_by(ano_mes_decisao) |>
  summarise(
    n = n(),
    media_dias = mean(tempo_dias, na.rm = TRUE),
    mediana_dias = median(tempo_dias, na.rm = TRUE),
    p90 = quantile(tempo_dias, 0.90, na.rm = TRUE),
    .groups = "drop"
  )

serie_tempo_status <- dados_resp |>
  group_by(ano_mes_decisao, status) |>
  summarise(
    n = n(),
    media_dias = mean(tempo_dias, na.rm = TRUE),
    mediana_dias = median(tempo_dias, na.rm = TRUE),
    .groups = "drop"
  )


ggplot(serie_tempo, aes(x = ano_mes_decisao, y = media_dias)) +
  geom_line() +
  geom_smooth(se = FALSE, method = "loess") +
  labs(
    title = "Tempo entre recebimento e decisão de admissibilidade – TJPR",
    x = "Data da decisão",
    y = "Dias (média)"
  )

ggplot(
  serie_tempo_status,
       aes(
         x = ano_mes_decisao, 
         y = media_dias, 
         color = status
       )
  ) +
  geom_line() +
  geom_smooth(
    se = FALSE, 
    method = "loess"
  ) +
  scale_color_viridis_d(option = "D", end = 0.85) +
  labs(
    title = "Quanto tempo a 1ª Vice Presidência do TJPR leva para realizar o juízo de admissibilidade de REsp",
    x = "Data da decisão",
    y = "Dias (média)",
    color = "Resultado",
    caption = "Fonte: DataJud/CNJ. Elaboração própria. Nota: tempo entre o recebimento do REsp (mov. 132) e a decisão de admissibilidade (movs. 430 e 433)."
  ) +
  ggthemes::theme_gdocs() +
  theme(
    plot.caption = element_text(
      size = 8,
      hjust = 0,                  # alinhado à esquerda
      margin = margin(t = 8)      # espaço acima da nota
    ),
    plot.title = element_text(face = "bold"),
    legend.position = "bottom"
  )



summary(dados_resp$tempo_dias)
nrow(dados_resp) / nrow(decisoes)

# analise a tendencia temporal de status ---------------------------------------

decisoes2015 <- decisoes |> filter(data_decisao >= as.Date("2015-01-01"))

prop_mensal <- decisoes2015 |>
  mutate(mes = floor_date(data_decisao, "month")) |>
  count(mes, status) |>
  group_by(mes) |>
  mutate(total = sum(n), prop = n / total) |>
  ungroup() |>
  filter(status == "Admitido")

ggplot(prop_mensal, aes(mes, prop)) +
  geom_point(aes(size = total), alpha = 0.4) +
  geom_line(alpha = 0.3) +
  geom_smooth(method = "loess", span = 0.3, aes(weight = total), se = TRUE) +
  scale_y_continuous(labels = scales::percent, limits = c(0, NA)) +
  labs(
    x = "Mês", y = "Taxa de Admissão",
    title = "Evolução da taxa de admissão (2015+)",
    size = "n/mês"
  ) +
  theme_minimal()

decisoes_bin <- decisoes |>
  mutate(
    y = as.integer(status == "Admitido"),
    t = as.numeric(difftime(data_decisao, as.Date("2015-01-01"), units = "days")) / 365.25
  )

fit <- glm(y ~ t, data = decisoes_bin, family = binomial)
summary(fit)
exp(coef(fit)["t"])       # OR por ano
exp(confint(fit, "t"))


library(splines)

# Modelo flexível com splines
fit_ns <- glm(y ~ ns(t, df = 5), data = decisoes_bin, family = binomial)
AIC(fit, fit_ns)

# Predição para visualização
pred_df <- tibble(t = seq(0, max(decisoes_bin$t), length.out = 200)) |>
  mutate(
    prob = predict(fit_ns, newdata = pick(everything()), type = "response"),
    data = as.Date("2015-01-01") + t * 365.25
  )

ggplot(prop_mensal, aes(mes, prop)) +
  geom_point(aes(size = total), alpha = 0.4) +
  geom_line(data = pred_df, aes(data, prob), color = "red", linewidth = 1) +
  scale_y_continuous(labels = scales::percent, limits = c(0, NA)) +
  labs(x = "Mês", y = "Taxa de Admissão",
       title = "Modelo com splines (ns, df=5)") +
  theme_minimal()


library(segmented)

fit_seg <- segmented(fit, seg.Z = ~t, npsi = 1)
summary(fit_seg)
#### nota: o breakpoint estimado te dá o momento da mudança estrutural




  #eof----------------------------------------------------------------------------