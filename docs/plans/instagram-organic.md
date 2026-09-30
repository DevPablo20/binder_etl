# Iniciativa: instagram-organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge. Reaproveita o molde do
`facebook_organic` (foto diária no bronze, schema explícito, sessão Spark em UTC) — a branch
parte de `arch/facebook-organic`.

## Próxima ação

Passo 1 — `src/transformers/instagram_organic/tables.py` com os schemas explícitos dos sete
streams, a partir dos arquivos dos syncs 39 (conexão principal) e 40 (stories), de 30/09.

## Objetivo

Levar ao lake o conteúdo orgânico das contas de Instagram — posts, stories e conta — em
camadas bronze/silver/gold, com o conector oficial do Airbyte como está. Dois ganhos sobre o
Facebook:

- **As métricas de post são orgânicas.** Conferido contra o Business Suite: o `reach` e as
  curtidas da API batem com total − anúncios.
- **As métricas da conta são uma série diária com histórico** — o `user_insights` é
  incremental e trouxe 30 dias no primeiro sync.

**Fora do escopo:**

- **Cópia ou conector próprio.** Como no Facebook: métricas e campos são os que o conector
  oficial pede.
- **`views` de imagem e carrossel.** O conector só pede `views` para Reels; o Business Suite
  tem o número, o lake não.
- **Orgânico × pago nas métricas da conta.** O `reach` do `user_insights` inclui anúncios, e o
  conector não pede o breakdown.
- **Integração com o backend** (Catalog API, Bridge, telas).
- **Ligar post do Instagram a post do Facebook.** Os posts cruzados são objetos diferentes nas
  duas APIs, sem id em comum. A ligação disponível é por conta: `users.page_id` = `page_id`
  do `facebook_organic`.

## Passos

| # | Passo | Status |
|---|---|---|
| 0 | Extração, seleção de campos, conexão de stories e diagnóstico com o Business Suite | feito 30/09 |
| 1 | `tables.py`: streams, schemas explícitos, chaves, métricas por tipo de mídia | pendente |
| 2 | Bronze: foto diária dos streams full refresh; série e última leitura nos demais | pendente |
| 3 | Silver: contas, séries da conta, mídia, fotos de mídia, stories, demografia | pendente |
| 4 | Gold: `media_daily_metrics`, `account_daily_metrics`, `story_metrics` | pendente |
| 5 | Testes: unitários por transform + conservação por mídia | pendente |
| 6 | Registro em `TRANSFORMERS`/`PLATFORMS`, fora do catálogo | pendente |
| 7 | DAG `instagram_organic_daily` | pendente |
| 8 | Desenho implementado sobe para `docs/`; plano apagado | pendente |

## Extração (Airbyte)

- **Conector:** Instagram oficial (Graph API v23.0 no manifest). **Um token para todas as
  contas** — a conexão enxerga as 5 contas de uma vez.
- **Destino:** `raw/airbyte/instagram_organic/{stream}/`. Sem pasta por conta: todo registro
  traz `business_account_id` (e o `page_id` da página do Facebook ligada).
- **Duas conexões no mesmo destino:**

| Conexão | Streams | Agendamento |
|---|---|---|
| principal | `users`, `user_insights`, `user_lifetime_insights`, `media`, `media_insights` | diário (o sync de 30/09 rodou às 02:01; alinhar à 01:00 do Facebook) |
| stories | `stories`, `story_insights` | de hora em hora, `0 0 * * * ?` (Quartz) |

- **Modos de sync:** `user_insights` é **Incremental + Append** (cursor `date`, releitura de
  1 dia); todos os outros são **Full Refresh + Append** — o conector não tem outra opção.
- **O stream `Api` fica desmarcado.** Ele só mapeia página → conta, o que o `users` já traz; o
  conector o usa internamente mesmo sem seleção.

Seleção de campos:

| Stream | Selecionados | Desmarcados e por quê |
|---|---|---|
| `users` | `id`, `username`, `name`, `page_id`, `followers_count`, `follows_count`, `media_count` | `biography`, `website`, `ig_id`; `profile_picture_url` expira |
| `user_insights` | todos: `date`, `reach`, `reach_week`, `reach_days_28`, `follower_count`, `online_followers`, `business_account_id`, `page_id` | — |
| `user_lifetime_insights` | todos: `metric`, `breakdown`, `value`, `business_account_id`, `page_id` | — |
| `media` | `id`, `ig_id`, `caption`, `permalink`, `timestamp`, `media_type`, `media_product_type`, `like_count`, `comments_count`, `is_comment_enabled`, `thumbnail_url`, `username`, `business_account_id`, `page_id` | `children` (uma chamada extra por item de carrossel), `owner` (repetido), `media_url` (expira; nulo em vídeo com áudio licenciado) |
| `media_insights` | todos, menos `total_interactions` | `total_interactions`: o conector nunca pede, vem sempre nulo |
| `stories` | `id`, `caption`, `permalink`, `shortcode`, `timestamp`, `media_type`, `media_product_type`, `thumbnail_url`, `business_account_id`, `page_id` | `like_count` (story não tem curtida), `media_url`, `owner`, `username`, `ig_id` |
| `story_insights` | todos: `reach`, `views`, `shares`, `follows`, `replies`, `profile_visits`, `total_interactions` + ids | — |

### Métricas de mídia por tipo

O conector pede métricas diferentes por tipo de mídia (fixo no manifest). **Nulo quer dizer
"não foi pedido para este tipo", não zero.** Curtidas e comentários vêm do stream `media`
(`like_count`, `comments_count`), que os traz para todos os tipos e é orgânico.

| Tipo | Pedido ao `/insights` | Nunca vem |
|---|---|---|
| REELS | `views`, `reach`, `likes`, `comments`, `saved`, `shares`, `ig_reels_avg_watch_time`, `ig_reels_video_view_total_time` | `follows`, `profile_visits` |
| FEED · IMAGE | `reach`, `saved`, `likes`, `comments`, `shares`, `follows`, `profile_visits` | `views`, tempos de reel |
| FEED · CAROUSEL_ALBUM | `reach`, `saved`, `shares`, `follows`, `profile_visits` | `likes`, `comments` (vêm do `media`), `views` |
| FEED · VIDEO (vídeo antigo) | `reach`, `saved` | todo o resto, menos curtidas e comentários do `media` |

## Desenho alvo

Slug `instagram_organic`, pasta `src/transformers/instagram_organic/` no molde de
`facebook_organic`. Cada tabela Delta guarda todas as contas, com `business_account_id` como
coluna.

### Bronze

Schema explícito por stream no `tables.py`, como no Facebook. `snapshot_date` com a mesma
regra: data em São Paulo de `_airbyte_extracted_at − 6h`.

| Stream | Chave de dedupe | Natureza |
|---|---|---|
| `users` | `id`, `snapshot_date` | foto diária |
| `user_lifetime_insights` | `business_account_id`, `metric`, `breakdown`, `snapshot_date` | foto diária (não tem `id`) |
| `media` | `id`, `snapshot_date` | foto diária |
| `media_insights` | `id`, `snapshot_date` | foto diária |
| `user_insights` | `business_account_id`, `date` | série nativa; vence a leitura mais recente |
| `stories` | `id` | última leitura |
| `story_insights` | `id` | última leitura — o valor final do story |

- **`user_insights` descarta linhas sem `date`.** O primeiro sync trouxe 4 linhas vazias por
  conta (só ids, nenhuma métrica).
- **Stories: só a última leitura.** As métricas de story só existem enquanto o story está no
  ar (24h); com o sync de hora em hora, a última leitura é de no máximo 1h antes de expirar e
  vale como o número final. As leituras horárias intermediárias não são guardadas.

### Silver

| Tabela | Grão | Origem |
|---|---|---|
| `accounts` | conta | última foto de `users`; `page_id` liga ao `facebook_organic` |
| `account_followers_snapshot` | conta × `snapshot_date` | `followers_count`, `follows_count`, `media_count` |
| `account_insights_daily` | conta × `metric_date` | `user_insights` |
| `follower_demographics_snapshot` | conta × `snapshot_date` × `breakdown` × chave | `user_lifetime_insights`, map aberto — fica só no silver |
| `media` | mídia | última foto de `media` |
| `media_insights_snapshot` | mídia × `snapshot_date` | `media_insights` + `like_count`/`comments_count` de `media` |
| `stories` | story | `stories` + `story_insights`, última leitura |

Regras do silver:

- **IDs como `string`, linhagem do Airbyte preservada** — as regras gerais do `CLAUDE.md`.
- **`metric_date` do `user_insights` = data de `date` em `America/Los_Angeles`, sem −1.** No
  Instagram, `date` é o **início** do dia (conferido contra o Business Suite) — o contrário do
  `end_time` das páginas do Facebook.
- **O dia mais recente está em andamento.** `is_partial` = a leitura aconteceu antes de o dia
  terminar (meia-noite seguinte em `America/Los_Angeles`). O sync seguinte relê e fecha o dia;
  sem a marca, o dia parcial parece queda de alcance.
- **`follower_count` do `user_insights` é o número de novos seguidores do dia**, não o total.
  O total vem de `followers_count` em `account_followers_snapshot`. O dia recente vem com 0
  até a Meta calcular.
- **`online_followers`** vira `MAP<STRING, BIGINT>` (hora do dia → seguidores online). Só há
  30 dias de histórico na API.
- **`likes` e `comments` de mídia vêm de `like_count` e `comments_count`** do stream `media`,
  para todos os tipos. Onde o `media_insights` também traz, bate em 1.881 de 1.882 mídias.
- **`format`** da mídia: `reel` (REELS), `carousel`, `image`, `video` (FEED · VIDEO).
- **`media_url` fica fora** (não selecionado); `permalink` identifica o post.

### Gold

**`media_daily_metrics`** — mídia × `snapshot_date`, no molde do `post_daily_metrics` do
Facebook: `_lifetime` e `_delta` para `likes`, `comments`, `reach`, `views`, `saved`,
`shares`, `follows`, `profile_visits`, com `baseline_kind`, `gap_days`, `hours_since_prev` e
as mesmas regras (primeira foto, buraco não interpolado, delta negativo mantido).

- **Métricas orgânicas.** Documentar na tabela: a Meta exclui interações em anúncios, e o
  `reach` bateu com total − anúncios no Business Suite.
- **Delta negativo é real e frequente**: comentário apagado, descurtida, salvamento desfeito, e
  `reach` estimado revisado para baixo (−3.368 num post das Loterias).
- **Métrica não pedida para o tipo** tem `_lifetime` e `_delta` nulos. Nunca vira 0.

**`account_daily_metrics`** — conta × `metric_date`: `reach` (total, inclui anúncios),
`reach_week`, `reach_days_28` (janelas móveis, não somam), `new_followers`, `is_partial`, e
`followers_count` da foto do mesmo dia.

**`story_metrics`** — um story por linha, com os valores finais: `reach`, `views`, `shares`,
`follows`, `replies`, `profile_visits`, `total_interactions`, a hora da última leitura e a
idade do story nela.

**Demografia dos seguidores não tem gold** — por ora fica só no silver.

**Os fatos não se somam entre si nem com o `facebook_organic`.** O alcance da conta inclui
anúncios e é único (não é a soma do alcance dos posts).

### Invariantes e testes

| Teste | Garante |
|---|---|
| **Conservação por mídia** | Foto inicial (se `pre_existing`) + Σ delta = última foto, métrica a métrica |
| Nulo por tipo | Carrossel sem `views` → `views_lifetime` e `views_delta` nulos, não 0 |
| Curtidas do carrossel | `likes` vem de `like_count` mesmo sem linha de `likes` no `/insights` |
| Data da conta | `date` 2026-09-26T07:00Z → `metric_date` 26/09; horário de inverno (08:00Z) também |
| Dia parcial | Leitura antes do fim do dia → `is_partial`; releitura depois → valor final, sem marca |
| Story | Várias leituras do mesmo story → uma linha, a mais recente |
| Fato órfão | Foto de mídia sem linha em `media` sobrevive ao `LEFT JOIN` |

## Decisões em aberto

| # | Decisão | Como fecha |
|---|---|---|
| D1 | **O `lifetime` perde o que tem mais de 2 anos?** A doc diz que as métricas ficam guardadas por até 2 anos. Um post do Binder de 2017 tem alcance 6 com 43 curtidas. Se o corte existir, a regra é a mesma da D1 do Facebook: delta só até 24 meses de idade | Fecha junto com a D1 do Facebook, depois de alguns dias de syncs: o post `18153850864324974` (Texaco, 30/09/2024 09:12, alcance 1.666) — se o alcance cair, o corte existe. Apoio: o post `17851618636187379` (Binder, 23/03/2017) no Business Suite |
| D2 | **Revisão tardia das métricas da conta.** O conector relê só o dia anterior; com o cron à 01:00, cada dia é lido pela última vez ~21h depois de terminar, e a Meta avisa revisão até 48h | Medir por alguns dias a diferença entre a penúltima e a última leitura de cada dia |

## Diário

**29/09** — Primeiro sync da conexão principal (sync 29, 16:07 São Paulo): 5 contas, 2.681
mídias, 170 linhas de `user_insights` (30 dias × 5 contas + 4 linhas vazias por conta),
7 stories e nenhum `story_insights`.

**30/09** — Sync 36 (02:01 São Paulo). Mesmas 2.681 mídias, nenhuma entrando ou saindo.
`user_insights` releu 28 e 29/09: o 28 igual; o 29 cresceu (Texaco 30.684 → 65.857) com
`follower_count` 0 — dia em andamento. Deltas entre os syncs: 10 mídias com menos
comentários, 8 com menos curtidas, 3 com menos salvamentos.

**30/09** — Leitura das documentações (Airbyte, manifest do conector, Graph API):

- `comments`, `likes`, `views` e `total_interactions` de mídia são só orgânicos; `like_count`
  exclui curtidas de posts promovidos.
- `reach` da conta inclui anúncios.
- `/media` devolve no máximo as 10 mil mídias mais recentes; métricas guardadas por até 2 anos.
- Métricas de story só por 24h; valores abaixo de 5 devolvem erro.
- `media_count` do `users` é maior que as mídias do `/media` em todas as contas (Texaco 1.728
  × 1.474). Não investigado.

**30/09** — Conferência com o Business Suite (só Instagram):

- `user_insights` da Texaco e das Loterias, 24–28/09: alcance e novos seguidores iguais à API,
  **no mesmo dia do `date`** — `date` é o início do dia. O Business Suite separa alcance pago
  e orgânico; a API bate com o total.
- Imagem das Loterias `18080678102370427`: curtidas, comentários, salvamentos e
  compartilhamentos iguais; `reach` caiu de 84.259 (API, 02:01) para 80.891; o Business Suite
  tem 137.847 visualizações, que a API não pede para imagem.
- Reel das Loterias `18116467333963584` e reel da Texaco `17870692002635891`: iguais, com o
  crescimento esperado pelo tempo.
- **Carrossel da Texaco `18196717822377553`: total 88.498 de alcance e 231 curtidas; anúncios
  85.365 e 31. A API tem `reach` 3.159 e `like_count` 200 — o orgânico.**

**30/09** — Seleção de campos refeita, stories movidos para conexão horária. Sync 39
(principal, 15:05 São Paulo): `like_count` e `comments_count` preenchidos nos 4 tipos de mídia;
`Api` sem arquivo. `user_insights` releu 28, 29 e 30/09 (Texaco 29/09 fechou em 66.930). Sync
40 (stories): 6 stories, 6 insights. O post da Texaco de 30/09/2024 ainda em 1.666 de alcance.

**30/09** — Decisões: stories guardam só a última leitura; demografia dos seguidores fica só no
silver (gold vai para o backlog); o corte de 2 anos (D1) fecha depois de alguns dias, junto com
o do Facebook; a revisão tardia da conta (D2) será medida como proposto.
