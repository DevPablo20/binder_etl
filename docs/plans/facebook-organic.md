# Iniciativa: facebook-organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge.

## Próxima ação

Validar a extração refeita com a seleção de campos nova (seção [Extração](#extração-airbyte)):
schema de cada stream, `id` presente no `page_insights`, `is_published`, `attachments`,
`shares`, `fan_count` e `followers_count` chegando. O resultado fecha os schemas explícitos do
passo 2.

## Objetivo

Levar ao lake o conteúdo das páginas do Facebook — posts e página — em camadas
bronze/silver/gold, reaproveitando a estrutura medallion do TikTok, **com o conector oficial do
Airbyte como está**. O ganho central é a **evolução diária das métricas de post**, que a fonte
só entrega como total acumulado.

**Fora do escopo:**

- **Cópia ou conector próprio.** Decisão tomada: trabalhar com o que o conector oficial
  entrega. Por isso não há `breakdown=is_from_ads` nem `since`/`until`.
- **Orgânico × pago nas visualizações.** Sem breakdown, não se separa. O gold chama as coisas
  pelo que são: desempenho total.
- **Marcar post impulsionado.** Nenhum campo do conector indica. Vai para o backlog via join
  com o Meta Ads.
- **Integração com o backend** (Catalog API, Bridge, telas). A plataforma não tem hierarquia
  campanha → anúncio.
- **Instagram.** Os posts da página são publicação cruzada do Instagram; o Facebook é só uma
  fatia do desempenho. Backlog.

## Passos

| # | Passo | Status |
|---|---|---|
| 0 | Extração, seleção de campos e diagnóstico com o Business Suite | feito 30/09 — falta validar a extração refeita |
| 1 | `tables.py`: streams, schemas explícitos, chaves, métricas | pendente |
| 2 | Bronze: foto diária de todas as streams | pendente |
| 3 | Silver: dimensões, séries e página diária | pendente |
| 4 | Gold: `post_daily_metrics` e `page_daily_metrics` | pendente |
| 5 | Testes: unitários por transform + conservação por post | pendente |
| 6 | Registro em `TRANSFORMERS`/`PLATFORMS` + 404 no catálogo para plataforma sem catálogo | pendente |
| 7 | DAG `facebook_organic_daily` | pendente |
| 8 | Desenho implementado sobe para `docs/`; plano apagado | pendente |

## Extração (Airbyte)

- **Conector:** Facebook Pages oficial (Graph API v24.0). Um `page_id` por conexão.
- **Streams:** `page`, `post`, `page_insights`, `post_insights` — todas **Full Refresh +
  Append**. O conector não oferece incremental.
- **Agendamento:** cron à **01:00 America/Sao_Paulo**.
- **Destino:** `raw/airbyte/facebook_organic/{page_id}/{stream}/`. Todas as conexões apontam
  para o mesmo prefixo; o `page_id` separa.

Seleção de campos:

| Stream | Selecionados | Observação |
|---|---|---|
| `page` | `id`, `name`, `username`, `link`, `category`, `fan_count`, `followers_count` | **Nunca** `page_token`. Nenhuma conexão (`feed`, `posts`, `photos`…) |
| `post` | `id`, `from`, `created_time`, `message`, `permalink_url`, `status_type`, `is_published`, `is_hidden`, `is_expired`, `attachments`, `shares`, `full_picture` | `type`, `name`, `description`, `caption`, `link`, `picture` são obsoletos. `insights` duplica o stream |
| `post_insights` | `id`, `name`, `period`, `values` | `id` = `{post_id}/insights/{metric}/{period}` |
| `page_insights` | `id`, `name`, `period`, `values` | `id` = `{page_id}/insights/{metric}/{period}` — não estava selecionado na primeira extração |

Métricas (fixas no manifest do conector, não configuráveis):

- `post_insights`: `post_media_view`, `post_total_media_view_unique`, `post_clicks`,
  `post_clicks_by_type`, `post_reactions_by_type_total`.
- `page_insights`: `page_total_actions`, `page_post_engagements`,
  `page_fan_adds_by_paid_non_paid_unique`, `page_media_view`, `page_total_media_view_unique`,
  cada uma em `day`, `week` e `days_28`.

## Desenho alvo

Slug `facebook_organic`, pasta `src/transformers/facebook_organic/` no molde da do TikTok. Cada
tabela Delta guarda todas as páginas, com `page_id` como coluna — não uma pasta por página.

### Bronze — uma foto por dia de todas as streams

**O bronze do TikTok deduplica pelo id e mantém a versão mais recente. Aqui isso apagaria o
histórico.** Todas as streams entram com `snapshot_date` na chave; o silver decide quem é
dimensão (última foto) e quem é série (todas as fotos). O volume é da ordem de milhares de
linhas por dia por página.

- **Leitura com schema explícito**, declarado por stream no `tables.py`:
  `spark.read.schema(SCHEMA).parquet(...)`. Sem `mergeSchema`. Sem nenhum dos dois, o Spark
  infere o schema de um único arquivo e, se escolher um antigo, colunas novas somem sem erro.
  Com schema explícito, coluna ausente num arquivo vira `NULL` e coluna extra é ignorada.
- **`page_id`** vem do caminho (`facebook_organic/{page_id}/`). Nos insights, o prefixo do `id`
  confirma.
- **`snapshot_date` = o dia que a foto fecha:**
  `to_date(from_utc_timestamp(_airbyte_extracted_at, 'America/Sao_Paulo') - INTERVAL 6 HOURS)`.
  O sync da 01:00 fecha o dia anterior; um sync manual à tarde cai no próprio dia e é
  substituído pelo da madrugada seguinte. Dois syncs com o mesmo `snapshot_date`: vence o mais
  recente (`BaseTransformer.dedupe`).
- **`page_insights` fica com o array `values` como veio.** Abrir o array é trabalho do silver.

| Stream | Chave de dedupe |
|---|---|
| `page` | `id`, `snapshot_date` |
| `post` | `id`, `snapshot_date` |
| `post_insights` | `id`, `snapshot_date` |
| `page_insights` | `id`, `snapshot_date` |

### Silver

| Tabela | Grão | Conteúdo |
|---|---|---|
| `pages` | página | Última foto: `page_id`, `page_name`, `username`, `link`, `category` |
| `page_followers_snapshot` | página × `snapshot_date` | `fan_count`, `followers_count` |
| `posts` | post | Última foto, filtrada por `from.id = page_id AND is_published`. `created_at` (UTC), `created_date` (São Paulo), `message`, `permalink_url`, `status_type`, `media_type` (de `attachments`), `is_hidden`, `is_expired`, `full_picture`, `last_seen_date` |
| `post_insights_snapshot` | post × `snapshot_date` | Uma coluna por métrica `lifetime` + `shares_lifetime` (do bronze de `post`) + `snapshot_at` |
| `page_insights_daily` | página × métrica × período × `metric_date` | Formato longo: `value` (inteiro) e `value_breakdown` (map) |

Regras do silver:

- **IDs como `string`.** Colunas de linhagem do Airbyte preservadas.
- **`created_time` já chega como timestamp em UTC.** Conversão para São Paulo só em `created_date`.
- **`last_seen_date`** = último `snapshot_date` em que o post apareceu. Como o stream é full
  refresh, sumir da lista significa sair da página — o contrário do incremental do TikTok.
- **`post_insights_snapshot` sai de um único `groupBy` com lista fixa de métricas** (no
  `tables.py`). Métrica nova na API não muda o schema.
- **Métricas em JSON** (`post_reactions_by_type_total`, `post_clicks_by_type`) viram
  `MAP<STRING, BIGINT>`. Map vazio `{}` = a API respondeu e não houve nada (a Meta só devolve
  chaves com valor diferente de zero). Map `NULL` = a métrica não veio.
- **`clicks_lifetime` = soma de `post_clicks_by_type`.** `post_clicks` é descartado: falta em
  cerca de um terço dos posts e, onde existe, é igual à soma do `by_type`.
- **Período `day` das métricas de post é descartado.** Vem sempre 0, com `end_time` congelado.
- **`page_insights_daily`: `metric_date = to_date(from_utc_timestamp(end_time,
  'America/Los_Angeles')) - 1`.** O `end_time` marca o fim da janela, à meia-noite do Pacífico.
  O mesmo `metric_date` chega em mais de um sync; vence a extração mais recente.
- **Checagem de buraco no `page_insights_daily`.** A API devolve só os 2 últimos dias
  disponíveis e o conector não aceita `since`. Se um `metric_date` faltar na sequência, a
  rodada avisa. O dia fica ausente — sem interpolação, sem como recuperar.

### Gold

**`post_daily_metrics`** — grão post × `snapshot_date`, partindo de
`post_insights_snapshot` com `LEFT JOIN` em `posts` e `pages`.

- Para cada métrica aditiva (`media_views`, `reach`, `clicks`, `reactions_total`, reações por
  tipo, `shares`): `{m}_lifetime` e `{m}_delta` (via `lag` sobre `snapshot_date`).
- Reações por tipo viram colunas fixas: `like` (inclui "care"), `love`, `haha`, `wow`, `sorry`,
  `anger`. Chave ausente num map presente = 0; map `NULL` = `NULL`.
- Contexto: `status_type`, `media_type`, `created_at`, `days_since_publish`,
  `prev_snapshot_date`, `gap_days`, `hours_since_prev`, `snapshot_at`, `baseline_kind`.
- **`baseline_kind`**, só na primeira foto de cada post:
  - `new_post` — `created_at >= snapshot_at - 1 dia`: a vida inteira do post é o delta;
  - `pre_existing` — histórico anterior desconhecido: delta `NULL`.
- **Buraco na série não é interpolado.** O delta cobre o intervalo inteiro; `gap_days` e
  `hours_since_prev` dizem quanto.
- **Delta negativo fica como veio.** Cortar em zero quebra a conservação.
- **`reach_delta` é alcance novo** (pessoas alcançadas pela primeira vez), não alcance do dia.
  A documentação da coluna precisa dizer isso.
- Documentação da tabela: **desempenho total do post no Facebook; em post impulsionado, inclui
  a distribuição paga.**

**`page_daily_metrics`** — grão página × `metric_date`, só o período `day`.

- `media_views`, `viewers`, `post_engagements`, `total_actions` — **total da página, dominado
  por anúncios.**
- `fan_adds_total`, `fan_adds_paid`, **`fan_adds_unpaid`** — a única separação orgânico × pago
  que o conector entrega.
- `fan_count` e `followers_count` vêm de `page_followers_snapshot`. Os dois grãos de data são
  diferentes (`snapshot_date` × `metric_date`); a regra de junção fica para o passo 4.

**Os dois fatos nunca se cruzam nem se somam.** O total da página inclui anúncios e conteúdo
que não está no feed de posts.

### Invariantes e testes

| Teste | Garante |
|---|---|
| **Conservação por post** | Para cada post e métrica aditiva: `lifetime` da foto inicial (se `pre_existing`) + `Σ delta` = `lifetime` da última foto |
| Bronze mantém fotos | Dois dias diferentes → 2 linhas; dois syncs no mesmo `snapshot_date` → 1 linha, a mais recente |
| Corte das 06:00 | Sync às 01:01 de D fecha D-1; sync às 17:35 de D fecha D |
| Silver da página | O mesmo `metric_date` vindo de dois syncs → 1 linha, valor mais recente; `end_time` 27/09 07:00Z → 26/09 |
| Map de reações | `{}` → colunas 0; map ausente → colunas `NULL` |
| Buraco | Uma foto faltando → `gap_days = 2`, sem linha inventada |
| Fato órfão | Foto de post sem linha em `posts` sobrevive ao `LEFT JOIN` |

### Encaixe no repositório

- Registrar em `TRANSFORMERS` e `PLATFORMS`, **sem** entrada em `CATALOG_BY_PLATFORM`. Hoje
  `src/api/routes/catalog.py` só checa `PLATFORMS`: `/catalog/facebook_organic` daria 500. A
  rota precisa devolver 404 para plataforma sem catálogo.
- Bronze próprio em `facebook_organic/bronze.py`, sem mexer em `base.py`.
- Testes em `tests/transformers/facebook_organic/`, espelhando a árvore de `src/`.

## Decisões em aberto

| # | Decisão | Como fecha |
|---|---|---|
| D1 | **A janela do `page_insights` avança um dia por dia?** Nos 4 syncs de 29–30/09 veio sempre o mesmo par (26 e 27/09), enquanto o Business Suite já mostrava 28 e 29/09 | Sync de 01/10 precisa trazer `end_time = 2026-09-29T07:00Z` (dia 28/09). Se não trouxer, a checagem de buraco vira alerta de rotina |
| D2 | **O `lifetime` perde o que tem mais de 2 anos?** A doc de Insights diz que `lifetime` é o período disponível, até 2 anos. Se sim, o delta fica negativo no aniversário de 2 anos e a regra passa a ser: delta só enquanto `snapshot_date < created_date + 24 meses`, com `is_beyond_retention` | Post 6 (`280663778456254_122161958420250905`, publicado 03/10/2024, 353 visualizações em 30/09) no sync de 04/10. Se cair, a regra entra; se não, sai |
| D3 | Junção de `fan_count`/`followers_count` (por `snapshot_date`) no `page_daily_metrics` (por `metric_date`) | Passo 4 |

## Diário

**29/09** — Primeira extração (sync 27): 1 página, 692 posts, 3.924 linhas de
`post_insights`, 15 de `page_insights`. Stream `post` só com `id`, `from`, `name`,
`description`, e `name`/`description` nulos. `page_insights` sem id de página.

**29–30/09** — Mais 3 syncs (28, 33, 34). O arquivo do sync 27 foi removido à mão antes de
mudar as colunas. Campos novos do `post` a partir do sync 33 (29/09 17:35). O sync 34 rodou às
01:01 (São Paulo), com o cron à 01:00. Os mesmos 692 posts em todos os syncs, publicados de
30/06/2024 a 29/09/2026. Nenhum delta negativo. Variação total entre 17:35 e 01:01: +115
visualizações nos 692 posts; o post novo de 29/09 foi 10 → 13 → 18 → 40.

**30/09** — Conferência com o Business Suite (aba Facebook):

- `page_media_view` com `end_time` 27/09 07:00Z = 842.533 = Business Suite **26/09**. Com 28/09
  07:00Z = 485.620 = **27/09**. Regra de −1 dia confirmada.
- Post publicado às 14:50:46 na API = 11:50 no Business Suite. `created_time` em UTC confirmado.
- Posts `…122270722622250905`, `…122270779214250905`, `…122141010062250905`,
  `…122161958420250905`: visualizações, espectadores e reações idênticos à API.
- Post `…122205957698250905`: 84.090 visualizações = **781 orgânicas + 83.309 de anúncios**.
  O `post_media_view` de post impulsionado inclui distribuição paga.
- Página, 25–29/09: 2.813.446 visualizações, sendo **6.400 orgânicas**.
- Espectadores maiores que visualizações acontece também no Business Suite (49 e 58 no post de
  29/09) — comportamento da Meta.
- A lista "Todos os conteúdos" soma Facebook e Instagram nos posts cruzados; comparar sempre
  pela aba Facebook do post.
- Espectadores muito abaixo das visualizações em posts de 2024 (178/3, 353/1). Não é o corte de
  2 anos — o post de 03/10/2024 ainda não tem 2 anos. Hipótese: `post_total_media_view_unique`
  é métrica nova (entrou no conector em 17/08/2026) e subestima posts antigos. Documentar na
  coluna; não corrigir.

**30/09** — Decidido não criar cópia do conector. Seleção de campos refeita e nova extração
disparada.
