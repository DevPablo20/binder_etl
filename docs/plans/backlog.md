# Backlog

Ideias ainda sem escopo. Quando uma delas for escopada, vira um arquivo de iniciativa em
`docs/plans/` e sai daqui.

- **Trigger automático do Airbyte no Airflow.** Hoje o sync é manual via `abctl`; a tarefa
  `sync_raw` do DAG é um placeholder.
- **Segundo fato para segmentações** (age, gender, region). Grão maior que ad × dia e
  mecanismo diferente do enriquecimento — não cabe no gold atual.
- **Retenção ou compactação do raw** quando o volume justificar. O bronze já acumula, então o
  raw pode ser podado sem perder histórico.
- **Checagem automática de documentação** (links quebrados, status fora de `docs/plans/`). Só
  se a checagem manual do fluxo de trabalho deixar passar coisa.
- **Marcar post impulsionado no `facebook_organic`.** O conector do Facebook Pages não indica
  impulsionamento e não separa orgânico × pago nas métricas de post. Quando o Meta Ads entrar
  no lake, o `effective_object_story_id` dos criativos tem o formato do `id` do post
  (`{page_id}_{post_id}`) — um `LEFT JOIN` marca `is_boosted` com base em gasto real.
- **Instagram orgânico.** Os posts das páginas do Facebook são publicação cruzada do
  Instagram, e a maior parte do desempenho orgânico está lá. O Facebook sozinho é uma fatia.
- **Kwai como segunda plataforma.** O raw já tem `raw/airbyte/kwai/` com os cinco streams
  extraídos; não há transformer, nem entrada no Bridge, nem tela. Decisão tomada em 16/09:
  **só depois do `bridge-enrichment` fechar de ponta a ponta** — a lógica de enriquecimento
  precisa estar provada em uma plataforma antes de virar molde para as outras. Quando entrar,
  é trabalho para a skill `add-platform`.
