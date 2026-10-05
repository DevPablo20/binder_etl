# Backlog

Ideias ainda sem escopo. Quando uma delas for escopada, vira um arquivo de iniciativa em
`docs/plans/` e sai daqui.

- **Trigger automático do Airbyte no Airflow.** Hoje o sync é manual via `abctl`; a tarefa
  `sync_raw` do DAG é um placeholder.
- **Destravar o `abctl local install` sem root.** O install aborta no pre-check de versão do
  Postgres porque não consegue ler `pgdata/PG_VERSION` (`drwx------`, uid 70) — detalhes e
  workaround em [airbyte/README.md](../../airbyte/README.md#known-blocker-abctl-local-install-fails-on-pgdata-permissions).
  O workaround via `sudo -E env HOME=...` está documentado mas **não foi executado de ponta a
  ponta**; falta validar, e decidir se a correção certa é rodar como root, ajustar o
  `securityContext` do chart ou mover a config para patch de ConfigMap como fluxo normal.
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
- **Demografia dos seguidores do Instagram no gold.** O `instagram_organic` guarda
  `follower_demographics` (cidade, país, idade × gênero; top 45 da Meta) só no silver, uma
  foto por dia. Falta decidir o fato e o uso.
- **Kwai como segunda plataforma.** O raw já tem `raw/airbyte/kwai/` com os cinco streams
  extraídos; não há transformer, nem entrada no Bridge, nem tela. Decisão tomada em 16/09:
  **só depois do `bridge-enrichment` fechar de ponta a ponta** — a lógica de enriquecimento
  precisa estar provada em uma plataforma antes de virar molde para as outras. Quando entrar,
  é trabalho para a skill `add-platform`.
