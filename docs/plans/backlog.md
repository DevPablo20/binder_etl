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
