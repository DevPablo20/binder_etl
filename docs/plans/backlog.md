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
- **Kwai como segunda plataforma.** O raw já tem `raw/airbyte/kwai/` com os cinco streams
  extraídos; não há transformer, nem entrada no Bridge, nem tela. Decisão tomada em 16/09:
  **só depois do `bridge-enrichment` fechar de ponta a ponta** — a lógica de enriquecimento
  precisa estar provada em uma plataforma antes de virar molde para as outras. Quando entrar,
  é trabalho para a skill `add-platform`.
