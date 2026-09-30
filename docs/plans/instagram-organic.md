# Iniciativa: instagram-organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge. Reaproveita o molde do
`facebook_organic` (foto diária no bronze, schema explícito, sessão Spark em UTC) — a branch
parte de `arch/facebook-organic`.

## Próxima ação

Fechar D1 junto com o Facebook e medir D2 com alguns dias de syncs. Com as duas fechadas,
apagar este plano e tirar o ponteiro do `CLAUDE.md`.

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
| 1 | `tables.py`: streams, schemas explícitos, chaves, métricas por tipo de mídia | feito 30/09 |
| 2 | Bronze: foto diária dos streams full refresh; série e última leitura nos demais | feito 30/09 |
| 3 | Silver: contas, séries da conta, mídia, fotos de mídia, stories, demografia | feito 30/09 |
| 4 | Gold: `media_daily_metrics`, `account_daily_metrics`, `story_metrics` | feito 30/09 |
| 5 | Testes: unitários por transform + conservação por mídia | feito 30/09 |
| 6 | Registro em `TRANSFORMERS`/`PLATFORMS`, fora do catálogo | feito 30/09 |
| 7 | DAG `instagram_organic_daily` | feito 30/09 |
| 8 | Desenho implementado sobe para `docs/`; plano apagado | desenho em `docs/` feito 30/09; plano fica até fechar D1 e D2 |

## Onde está o desenho

- Conexões, seleção de campos, agendamento e a regra de Append: `airbyte/README.md`, seção
  Instagram.
- Fluxo, métricas por tipo de mídia, regras de cada camada e conservação:
  `docs/architecture.md`, seção Instagram orgânico.
- Estrutura, chaves por stream e o módulo compartilhado com o Facebook
  (`src/transformers/snapshots.py`): `docs/project-structure.md`.
- Regras que valem sempre: `CLAUDE.md` (foto diária nas plataformas orgânicas; Airbyte sempre em
  Append).

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

**30/09** — Passos 1 a 7 implementados e rodados contra o MinIO de `192.168.10.80`:

- Bronze: `media` e `media_insights` com 2 fotos (29/09 e 30/09, 2.681 cada);
  `user_insights` com 155 linhas (31 dias × 5 contas), sem as linhas vazias; stories com 8.
- Silver: 5 contas; 2.681 mídias (1.050 reels, 832 imagens, 665 carrosséis, 134 vídeos);
  974 linhas de demografia; `account_insights_daily` com o dia 30/09 marcado como parcial.
- Gold `media_daily_metrics`: 5.362 linhas. Em 29/09, 2.678 `pre_existing` e 3 `new_post`; em
  30/09, +15.621 de alcance e +3.328 visualizações de reels. Nulos exatamente onde o conector
  não pede a métrica. O carrossel `18196717822377553` sai com `likes` 200 em 30/09 e sem delta
  (29/09 não tinha `like_count` nem `likes`).
- Gold `account_daily_metrics`: seguidores entram a partir de 29/09, a primeira foto de `users`.
  `new_followers` do dia 29/09 ainda 0 às 15:05 de 30/09 — vale para a medição de D2.
- Gold `story_metrics`: 8 stories; um do Binder sem insights (menos de 5 visualizações).
- **A conexão de stories está em Overwrite.** No raw de `stories/` e `story_insights/` sobrou só
  o arquivo das 19:01; os de 29/09 e de 30/09 de manhã foram apagados pelos syncs horários.
- O cálculo de delta e a foto diária foram extraídos para `src/transformers/snapshots.py`, e o
  `facebook_organic` passou a usá-los (testes do Facebook verdes).
- Testes: 51 dos orgânicos verdes contra o MinIO remoto; suíte inteira verde no local
  (72 passed, 4 skipped — as conservações orgânicas, sem dados ali).

**30/09** — Conexão de stories trocada para Full Refresh + Append. O histórico de stories começa
a partir daqui.
