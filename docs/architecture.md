# Arquitetura — lake, enriquecimento e publicação

Invariantes que governam tudo isso: [../CLAUDE.md](../CLAUDE.md). Modelo Bridge no lado do
backend: `binder_app_backend/docs/architecture.md`.

## Três zonas

| Zona | Dono | Responsabilidade |
|---|---|---|
| Lake (`raw` → `gold`) | ETL / MinIO | Fatos com IDs nativos de plataforma |
| Catalog API | FastAPI do ETL (`src/api/`) | Identidades da hierarquia, lidas do silver — alimenta a descoberta do Bridge |
| Metadados do Bridge | Postgres do backend | Vínculos e classificações (SSOT do backend) |

```mermaid
flowchart LR
    Airbyte["Airbyte abctl"] --> Raw["raw/airbyte/{platform}/{stream} — acumula"]
    Raw --> Bronze["bronze — acumula + dedupe"]
    Bronze --> Silver["silver/{platform}/{table}"]
    Silver --> Gold["gold/{platform}/ads_daily_metrics"]
    Silver --> CatalogBuild["linhas de catálogo"]
    CatalogBuild --> CatalogAPI["FastAPI GET /catalog"]
    Backend["Bridge — configuração"] --> CatalogAPI
    Backend --> Publication["GET /enrichment/publications/pending"]
    Publication --> GoldEnriched["gold enriquecido"]
    Gold --> GoldEnriched
```

## Extração (Airbyte)

Configuração do conector TikTok Marketing:

| Stream | Modo de sync |
|---|---|
| `advertisers` | Full Refresh + Append |
| `campaigns`, `ad_groups`, `ads` | Incremental + Append |
| `ads_reports_daily` | Incremental + Append |

- **Include Deleted Data** ligado — sem ele, objetos deletados nunca chegam às dimensões,
  mas as métricas deles chegam ao relatório.
- **`start_date` 2025-01-01.** As streams incrementais só trazem objetos modificados desde
  essa data; objeto não tocado desde antes dela não entra na dimensão.
- **Mudar `start_date` ou Include Deleted exige Clear data dos streams.** O cursor do
  incremental vive no Airbyte, não no MinIO — limpar o raw não o reseta, e o próximo sync
  continua de onde parou.

Comportamentos do conector que o pipeline precisa absorver:

- **`advertiser_id` vem `NULL`** no `ads_reports_daily`, tanto no nível superior quanto em
  `dimensions.advertiser_id`. O fato só traz `ad_id`, `metrics.adgroup_id` e
  `metrics.campaign_id`; a conta deriva da hierarquia.
- **Uma linha por ad por dia, mesmo sem entrega.** Cerca de dois terços do relatório têm
  todas as métricas zeradas — ver [Linhas sem atividade](#linhas-sem-atividade).
- **Reextração do mesmo ad × dia** entre syncs incrementais — o dedupe do bronze é
  obrigatório, não otimização.

**Completude e conservação são problemas diferentes.** A extração decide *o que* chega ao
lake; os joins decidem se *o que chegou* sobrevive até o gold. O `LEFT JOIN` garante o
segundo, nunca o primeiro — dimensão faltando por extração incompleta aparece como `NULL`,
não como erro.

## Existência de objeto

Deletados chegam explicitamente em `secondary_status` — `CAMPAIGN_STATUS_DELETE`,
`ADGROUP_STATUS_DELETE`, `ADGROUP_STATUS_CAMPAIGN_DELETE`, `AD_STATUS_DELETE`,
`AD_STATUS_ADGROUP_DELETE`, `AD_STATUS_CAMPAIGN_DELETE`.

**"Ainda existe na plataforma?" se responde pelo status, não por `_airbyte_extracted_at`.**
No incremental um objeto só é reextraído quando muda, então `extracted_at` é a última
*modificação vista*, não a última vez que o objeto existiu.

## A acumulação no bronze

O raw acumula um arquivo por sync, mas o bronze não depende disso: ele é a camada
acumuladora. Isso protege o histórico se o raw ganhar retenção ou compactação, ou se algum
stream — desta plataforma ou de uma futura — só oferecer Overwrite.

Em `bronze.py`, a leitura do bronze existente e a decisão de unir ficam separadas
(`_read_existing_bronze` faz I/O, `_accumulate` é pura):

```python
new = read_raw_parquet(spark, stream.raw_path)
existing = self._read_existing_bronze(stream)      # None na primeira execução
combined = self._accumulate(existing, new)          # existing.unionByName(new, ...) ou só new

cleaned = self.dedupe(combined, list(stream.dedupe_columns))
cleaned.cache()
cleaned.count()                       # materializa antes de sobrescrever a origem
write_delta(cleaned, "bronze", stream.bronze_table_name, mode="overwrite")
```

`BaseTransformer.dedupe` ordena por `_airbyte_extracted_at desc`, então a versão mais
recente vence — o SCD tipo 1, entre rodadas e dentro de uma rodada.

`tests/transformers/tiktok/bronze/test_accumulate.py` prova o cenário que importa sem MinIO:
um objeto presente no bronze mas ausente do raw desta rodada sobrevive a `_accumulate` +
`dedupe`.

### Chave de dedupe: a chave natural mínima

O raw e o bronze acumulam versões de cada objeto. Com chave composta, um ad que mudasse de
ad_group teria duas versões com chaves diferentes, o dedupe não colapsaria, e a métrica
duplicaria no join do gold. Por isso cada stream deduplica só pelo próprio id:

| Stream | Chave de dedupe |
|---|---|
| `advertisers` | `advertiser_id` |
| `campaigns` | `campaign_id` |
| `ad_groups` | `adgroup_id` |
| `ads` | `ad_id` |
| `ads_reports_daily` | `ad_id, stat_time_day` |

### Outras ressalvas

- **Materialize antes de escrever** (`.cache()` + `.count()`). Lê-se e sobrescreve-se o mesmo
  caminho; o Delta resolve por snapshot isolation, mas forçar a avaliação elimina surpresa de
  lazy evaluation.
- **Read-modify-write relê a tabela inteira** a cada rodada. Em centenas de objetos é
  irrelevante; se virar milhões, aí sim vale um `MERGE`. É problema de escala, não de
  correção.

## Linhas sem atividade

O transform silver de `ads_reports_daily` descarta ad × dia em que **todas** as métricas
numéricas são nulas ou zero. Uma linha com spend 0 mas alguma conversão sobrevive. Nenhuma
soma muda, porque o que sai já era zero — o teste de conservação cobre bronze × silver.

Unitário: `tests/transformers/tiktok/silver/test_ads_reports_daily_filter.py`.

## Gold partindo do fato

Em `transforms/gold/ads_daily_metrics.py`:

- **Base é o fato** (`ads_reports_daily`); `ads`, `ad_groups`, `campaigns` e `advertisers`
  entram por `LEFT JOIN`.
- **Chaves de hierarquia vêm do fato** (`campaign_id`, `ad_group_id`, `ad_id`), que sempre as
  carrega — não das dimensões, que podem faltar.
- **`ad_account_id` vem da dimensão `ads`**, porque o fato não o traz. Linha sem dimensão
  `ads` fica com conta `NULL`, mesmo que a campanha esteja vinculada.
- **Atributo de dimensão ausente vira `NULL` no gold base**, não `'Não informado'`. O balde
  explícito é responsabilidade do gold enriquecido; no gold base, `NULL` é o registro preciso
  de "faltou a dimensão".

**Função pura, sem I/O.** `transform(sources, fact)` recebe um dict `{nome do silver source:
DataFrame}` já carregado e devolve o gold. Quem faz I/O é `TikTokGoldTransformer` em
`gold.py`, que lê cada silver source uma vez. É o que torna a regra testável sem MinIO:
`tests/transformers/tiktok/gold/test_ads_daily_metrics.py` constrói uma linha de fato órfã,
sem nenhuma dimensão, e prova que ela sobrevive. Esse teste é a garantia do `LEFT JOIN` — o
de conservação sozinho não basta, porque o filtro do silver remove os órfãos vazios que
exporiam um `inner join`.

## Contrato de join com o Bridge

`BridgeMapping` em `tables.py` documenta quais colunas do silver são chave de join para o
Bridge. Não é dado de mapeamento — é o contrato de "qual coluna do lake casa com qual coluna
do Bridge".

| Coluna silver | Alvo no Bridge |
|---|---|
| `ad_account_id` | `platform_account.external_account_id` |
| `campaign_id` | `platform_campaign_binding.external_campaign_id` |
| `ad_group_id` | `platform_ad_group_classification.external_ad_group_id` |
| `ad_id` | `platform_ad_classification.external_ad_id` |

No fato, `ad_account_id` é derivado (ver [Gold partindo do fato](#gold-partindo-do-fato)),
não nativo.

## Gold enriquecido

Desenho alvo:

```
gold_enriched = ads_daily_metrics
    LEFT JOIN enrichment_campaign  ON (ad_account_id, campaign_id)
    LEFT JOIN enrichment_ad_group  ON (ad_account_id, ad_group_id)
    LEFT JOIN enrichment_ad        ON (ad_account_id, ad_id)
```

Três `LEFT JOIN` em três chaves diferentes. **Sem `coalesce`, sem regra de precedência, sem
resolução de conflito** — porque nenhum atributo é declarado em dois níveis. A herança
acontece sozinha: a linha do fato é ad × dia e já carrega `campaign_id` e `ad_group_id`, então
o atributo declarado acima desce pelo próprio join.

Como `ad_account_id` é derivado no fato, uma linha sem dimensão `ads` não casa com um
enriquecimento que use a conta na chave, mesmo que a campanha esteja vinculada.

### Eixos dinâmicos viram um MAP, não colunas

Os eixos são declarados por campanha, então pivotar para colunas faria o schema do gold mudar
toda vez que uma campanha ganhasse um eixo. Use `enrichment MAP<STRING, STRING>` — o filtro
fica `enrichment['Território'] = 'Canais'`, sem churn de schema e com sintaxe única para
qualquer eixo.

### Formato traduzido

O backend entrega a tradução `(platform, native_value) → (format, sub_format)` junto da
publicação. Valor nativo sem tradução **nunca quebra a rodada**: o ad recebe `'Não mapeado'`
e o gold carrega `format_native_value` com o valor cru, para o backend montar a fila de
pendências ordenada por investimento afetado.

`ad_format` do TikTok tem cardinalidade muito baixa:

- `SINGLE_VIDEO` concentra a maior parte do investimento;
- `CAROUSEL_ADS` inclui os anúncios ACO (`is_aco = true`);
- **nulo** é valor legítimo: são os posts autorizados (`identity_type = BC_AUTH_TT`).

`ad_format` distingue vídeo de carrossel, mas **não carrega duração** — não separa 15s de 30s.

## Invariante de conservação

> A soma de qualquer métrica no gold enriquecido, sem filtro, tem que ser idêntica à soma no
> fato cru. Se divergir, o enriquecimento está comendo dado.

- Todo join é `LEFT`, sem exceção — no gold base e no enriquecido.
- `NULL` vira balde explícito no enriquecido: `'Não informado'` é categoria legítima que
  aparece nos gráficos, não filtro implícito.
- **A invariante é teste automático.** `tests/transformers/tiktok/test_conservation.py`
  compara linhas, `spend`, `impressions` e `clicks` entre bronze × silver e silver × gold.
  Divergência para menos é join descartando (ou filtro cortando linha com atividade); para
  mais é fan-out.
- **Linha não é dinheiro.** Meça a divergência pela soma de métrica antes de dimensionar o
  problema: uma perda grande em linhas pode ser quase nada em spend, e vice-versa.

## Publicação

Configurar no backend não dispara processamento. Publicar sim, uma vez para o lote inteiro.

O DAG busca `GET /enrichment/publications/pending` no backend e usa **aquele snapshot**, nunca
as tabelas vivas — isso torna a rodada reprodutível mesmo com alguém editando. O gold
enriquecido carrega `enrichment_publication_id`, o que dá o rastro de auditoria que o SCD
tipo 1 não guarda: não se sabe *quando* um ad_group mudou de território, mas se sabe que a
publicação #7 gerou aqueles números e a #8 gerou estes.

Transporte por HTTP e não por JDBC ou S3: é simétrico ao `catalog-api` que já existe na
direção oposta, não exige credencial S3 no backend nem driver JDBC no Spark. O snapshot é da
ordem de mil linhas de JSON — `spark.createDataFrame()` resolve. Se crescer, troca-se por
parquet no MinIO sem mudar o modelo.

## Facebook orgânico (`facebook_organic`)

Conteúdo das páginas do Facebook — posts e página —, com o conector oficial do Airbyte como
está. Não passa pelo Bridge nem pela Catalog API: não existe hierarquia campanha → anúncio.

### O que a fonte entrega

- **Quatro streams, todos Full Refresh + Append** (`page`, `post`, `post_insights`,
  `page_insights`). Cada sync é uma foto completa; o conector não tem incremental.
- **Métricas de post só como total acumulado** (`lifetime`). A evolução diária não existe na
  fonte: é a diferença entre duas fotos. **Histórico que não foi fotografado não se recupera.**
- **Métricas de página por dia, só os 2 últimos dias disponíveis**, com uns 2 dias de atraso.
  O conector não aceita `since`: cada dia passa por 2 syncs diários, e duas falhas seguidas
  abrem um buraco permanente.
- **Nada separa orgânico de pago nas visualizações.** A Meta tem o breakdown `is_from_ads`,
  mas a lista de métricas é fixa no manifest do conector. Num post impulsionado, as
  visualizações são quase todas de anúncio; o total da página é dominado por anúncio.
- **O Business Suite e a API leem a mesma fonte.** Os números da aba Facebook de um post batem
  exatamente com o `post_insights`; a lista "Todos os conteúdos" soma Facebook e Instagram
  nos posts cruzados e não serve de comparação.

### Bronze: uma foto por dia

A regra do TikTok — dedupe pelo id, vence a versão mais recente — aqui apagaria o histórico.
Todos os streams deduplicam por `id + snapshot_date`, e o silver decide quem é dimensão
(última foto) e quem é série (todas as fotos).

- **`snapshot_date` é o dia que a foto fecha:** a data em São Paulo de
  `_airbyte_extracted_at − 6h`. O sync da 01:00 fecha o dia anterior; um sync manual à tarde
  cai no próprio dia e é substituído pelo da madrugada seguinte.
- **Raw lido com schema explícito** (`tables.py`). Sem ele, o Spark infere o schema de um
  arquivo só; se pegar um anterior a uma mudança na seleção de campos, colunas novas somem
  sem erro.
- **`page_id` vem do caminho** `raw/airbyte/facebook_organic/{page_id}/{stream}/`. Fotos
  antigas do `page_insights`, sem `id`, têm o id reconstruído no formato da API
  (`{page_id}/insights/{metric}/{period}`) — sem isso, as métricas de uma foto teriam a mesma
  chave nula.

### Silver

| Tabela | Grão | Origem |
|---|---|---|
| `pages` | página | última foto de `page` |
| `page_followers_snapshot` | página × `snapshot_date` | `fan_count`, `followers_count` de `page` |
| `posts` | post | última foto de `post`, só da própria página e publicado |
| `post_insights_snapshot` | post × `snapshot_date` | `post_insights` (`lifetime`) + `shares` de `post` |
| `page_insights_daily` | página × métrica × período × `metric_date` | `page_insights`, array aberto |

- **Map vazio e map nulo são coisas diferentes.** As métricas por tipo (reações, cliques)
  viram `MAP<STRING, BIGINT>`. A Meta só devolve chaves com valor, então `{}` vale zero; map
  nulo é métrica que não veio.
- **`clicks` é a soma de `post_clicks_by_type`.** `post_clicks` falta em cerca de um terço
  dos posts e, onde existe, é igual à soma.
- **`shares` ausente vale 0** — a Meta omite o campo em post sem compartilhamento —, exceto
  em fotos de antes da seleção do campo, em que é desconhecido.
- **O formato do post sai do link e do `status_type`** (`reel` pelo `/reel/` no permalink).
  O `attachments` do conector chega vazio.
- **`metric_date` é o dia anterior ao `end_time` no Pacífico.** O `end_time` é a meia-noite
  que encerra o dia medido. O silver aponta dias faltando na sequência, sem interpolar.

### Gold

**`post_daily_metrics`** — post × `snapshot_date`. Cada métrica aditiva sai como `_lifetime`
e `_delta`:

- **Primeira foto de um post:** `new_post` (publicado nas 24h anteriores) tem a vida inteira
  como delta; `pre_existing` tem histórico desconhecido e delta nulo.
- **Buraco não é interpolado.** O delta cobre o intervalo inteiro; `gap_days` e
  `hours_since_prev` dizem quanto.
- **Delta negativo fica como veio.** Cortar em zero quebra a conservação.
- **`reach_delta` é alcance novo**, não alcance do dia. E `reach` subestima muito posts
  antigos: a métrica única é recente na API.

**`page_daily_metrics`** — página × `metric_date`, só o período `day`. `fan_adds_unpaid` é a
única separação orgânico × pago que a fonte entrega. Seguidores entram pela foto do mesmo dia.

**Os dois fatos nunca se somam nem se cruzam.** O total da página inclui anúncios e conteúdo
fora do feed de posts.

### Conservação

Duas garantias em `tests/transformers/facebook_organic/test_conservation.py`: cada foto do
silver vira uma linha do gold com o mesmo total; e, por post e métrica, a foto inicial (se
`pre_existing`) mais a soma dos deltas é igual ao total da última foto.

## Instagram orgânico (`instagram_organic`)

Conteúdo orgânico das contas de Instagram — posts, stories e conta —, com o conector oficial do
Airbyte como está. Mesmo molde do `facebook_organic`: fora do Bridge e da Catalog API, raw lido
com schema explícito, foto diária no bronze. As regras de foto e delta são compartilhadas em
`src/transformers/snapshots.py`.

### O que a fonte entrega

- **Um token enxerga todas as contas.** O raw fica em `raw/airbyte/instagram_organic/{stream}/`,
  sem pasta por conta; todo registro traz `business_account_id` e o `page_id` da página do
  Facebook ligada — a ponte com o `facebook_organic`. Posts cruzados são objetos diferentes nas
  duas APIs, sem id em comum.
- **As métricas de post são orgânicas.** A Meta exclui interações em anúncios de `likes`,
  `comments` e `views`, e o `like_count` exclui curtidas de posts promovidos. O `reach` de post
  bate com total − anúncios no Business Suite.
- **As métricas da conta incluem anúncios.** O `reach` do `user_insights` é o total; o
  conector não pede o breakdown que separaria.
- **Métricas de mídia por tipo.** O conector pede um conjunto por tipo (fixo no manifest):
  `views` só para Reels; `follows` e `profile_visits` nunca para Reels; `likes` e `comments`
  nunca para carrossel; vídeo de feed antigo só `reach` e `saved`. **Métrica não pedida vem
  nula e continua nula** — nunca vira 0. Curtidas e comentários de todos os tipos vêm do
  `like_count`/`comments_count` do stream `media`.
- **A conta é uma série diária com histórico.** O `user_insights` é incremental, traz até 30
  dias e relê o dia anterior a cada sync.
- **Stories só existem por 24h**, e as métricas deles também. A conexão de stories roda de
  hora em hora; a última leitura é o número final.
- **Limites da API:** o `/media` devolve as 10 mil mídias mais recentes por conta; as métricas
  ficam guardadas por até 2 anos.

### Bronze

| Stream | Chave de dedupe | Natureza |
|---|---|---|
| `users`, `media`, `media_insights` | `id`, `snapshot_date` | foto diária |
| `user_lifetime_insights` | `business_account_id`, `metric`, `breakdown`, `snapshot_date` | foto diária |
| `user_insights` | `business_account_id`, `date` | série nativa; vence a leitura mais recente |
| `stories`, `story_insights` | `id` | só a última leitura |

O `user_insights` descarta linhas sem `date` — o primeiro sync traz linhas vazias, só com ids.

### Silver

| Tabela | Grão | Origem |
|---|---|---|
| `accounts` | conta | última foto de `users` |
| `account_followers_snapshot` | conta × `snapshot_date` | `followers_count`, `follows_count`, `media_count` |
| `account_insights_daily` | conta × `metric_date` | `user_insights` |
| `follower_demographics_snapshot` | conta × `snapshot_date` × `breakdown` × valor | `user_lifetime_insights`; só no silver |
| `media` | mídia | última foto de `media`, com `format` (`reel`, `carousel`, `image`, `video`) |
| `media_insights_snapshot` | mídia × `snapshot_date` | `media_insights` + `like_count`/`comments_count` de `media` |
| `stories` | story | `stories` + `story_insights`, última leitura |

- **`metric_date` é a própria data do `date`, no Pacífico — sem −1.** No Instagram o `date` é a
  meia-noite que **inicia** o dia, o contrário do `end_time` das páginas do Facebook.
- **O dia mais recente está em andamento.** `is_partial` marca a leitura feita antes de o dia
  terminar; a releitura do sync seguinte fecha o dia.
- **`follower_count` da API é o número de novos seguidores do dia** (`new_followers`). O total
  vem da foto de `users`. O dia recente vem com 0 até a Meta calcular.
- **Story sem insights sobrevive.** A Meta devolve erro para métrica de story com valor menor
  que 5; `hours_live_at_last_read` diz com que idade o story foi lido pela última vez.

### Gold

- **`media_daily_metrics`** — mídia × `snapshot_date`, `_lifetime` e `_delta` com as regras de
  `snapshots.py`. Orgânico. Delta negativo é frequente e real: comentário apagado, descurtida,
  salvamento desfeito, `reach` estimado revisado para baixo.
- **`account_daily_metrics`** — conta × `metric_date`: `reach` (total, inclui anúncios),
  `reach_week` e `reach_days_28` (janelas móveis, não somam), `new_followers`, `is_partial` e os
  totais da foto do mesmo dia.
- **`story_metrics`** — um story por linha, com os números da última leitura.

O alcance da conta nunca se soma ao dos posts: é único e inclui anúncios.

### Conservação

`tests/transformers/instagram_organic/test_conservation.py`, com as mesmas duas garantias do
`facebook_organic`, por mídia.
