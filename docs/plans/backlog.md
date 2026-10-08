# Backlog

Ideias ainda sem escopo. Quando uma delas for escopada, vira um arquivo de iniciativa em
`docs/plans/` e sai daqui.

- **Alerta automático de falha de DAG.** Hoje o acompanhamento é manual, pela UI do Airflow
  (`localhost:8081`), com `retries: 1` e silêncio no resto. Decisão de 07/10: começar simples e
  só automatizar se a vigilância manual deixar passar algo. Quando entrar, é
  `on_failure_callback` nos `default_args` dos DAGs do orgânico — falta escolher o canal.
  Vale junto: **persistir os logs do Airflow** (`/opt/airflow/logs` não é volume montado, então
  `docker compose up -d` apaga o log das tarefas e a UI mostra a falha sem o motivo).
- **Trigger automático do Airbyte nas plataformas pagas** (Kwai, Facebook Marketing, TikTok).
  O `sync_raw` desses DAGs é um placeholder e o acoplamento com a extração é por horário. As
  orgânicas saíram daqui: estão escopadas em
  [organic.md](organic.md), que serve de molde quando estas entrarem.
- **Destravar o `abctl local install` sem root.** O install aborta no pre-check de versão do
  Postgres porque não consegue ler `pgdata/PG_VERSION` (`drwx------`, uid 70) — detalhes e
  workaround em [airbyte/README.md](../../airbyte/README.md#known-blocker-abctl-local-install-fails-on-pgdata-permissions).
  O workaround via `sudo -E env HOME=...` está documentado mas **não foi executado de ponta a
  ponta**; falta validar, e decidir se a correção certa é rodar como root, ajustar o
  `securityContext` do chart ou mover a config para patch de ConfigMap como fluxo normal.
- **Segundo fato para segmentações** (age, gender, region). Grão maior que ad × dia e
  mecanismo diferente do enriquecimento — não cabe no gold atual.
- **Retenção do raw** quando o volume justificar — o que, com `max_padding_size_mb: 0`, deixou
  de ser logo. O bronze acumula, então o raw pode ser podado sem perder histórico, mas a poda
  não é só apagar arquivo: com `ETL_STRICT` ligado, raw vazio ou ausente **falha** a tarefa, e
  para um stream podado isso é o estado normal, não erro. A política precisa primeiro dar ao
  bronze como distinguir "o raw nunca chegou" de "o raw foi podado e eu já tenho". Enquanto
  isso não existir, qualquer poda manual preserva o objeto mais recente de cada stream, para o
  caminho não desaparecer. Vale também lembrar que, apagado o raw orgânico, o bronze fica
  **cópia única** de foto que a Meta não reentrega — então backup do bronze vem antes da poda,
  e é barato porque o ativo é pequeno.
- **Checagem de frescor nas plataformas pagas.** O orgânico tem `assert_photo_reached_gold` e
  `require_all_pages_extracted`; o pago não tem nada, e o `sync_raw` do `tiktok_daily` ainda é
  `EmptyOperator`. O custo disso já apareceu uma vez: o bronze do TikTok ficou dois dias atrás
  do raw e a gold ficou 571 fatos ad × dia mais curta, com todas as folhas verdes.
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
- **Caminho para coluna nova em bronze/silver.** O `overwrite` do Delta recusa schema
  diferente, e não havia como acrescentar coluna: acrescentar `is_published` ao
  `silver/facebook_organic/posts` exigiu uma escrita pontual com `overwriteSchema`. O
  `write_delta` agora tem `overwrite_schema`, desligado por default — ligado sempre, um
  transform que deixasse de produzir uma coluna a apagaria da tabela em silêncio em vez de
  falhar. Falta decidir se, nas camadas de overwrite full (onde o schema é derivado do
  código, não contrato com histórico), o default certo não é o contrário, e onde mora o
  passo de migração: hoje é script de sandbox, que não deixa rastro.
