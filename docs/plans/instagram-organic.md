# Iniciativa: instagram-organic

> **Estado de trabalho, não referência.** Este arquivo diz o que falta, o que está em aberto
> e o que aconteceu no caminho. Como o sistema funciona está no `CLAUDE.md` e em `docs/`.
> Quando a iniciativa encerrar, o desenho implementado sobe para `docs/architecture.md` e
> `docs/project-structure.md`, e este arquivo é apagado — o histórico fica no git.

Iniciativa só do `binder_etl`. Nada aqui toca o backend nem o Bridge. Reaproveita o molde do
`facebook_organic` (foto diária no bronze, schema explícito, sessão Spark em UTC) — a branch
parte de `arch/facebook-organic`.

## Próxima ação

D1 medida e **fechada em 06/10** (diário): o corte existe no `reach`, mas não é por
aniversário de post — a regra que a D1 propunha não se aplica. Fica a **D3** (horizonte por
métrica dentro do pipeline, compartilhada com o Facebook) e a **D2**, que agora tem 7 dias de
fotos e pode ser medida. A **D4** (hora do sync de stories) nasceu e fechou em 06/10.

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
| 4 | Gold: `media_daily_metrics`, `account_daily_metrics`, `story_metrics` | feito 30/09 — substituída pela gold orgânica compartilhada (ver diário) |
| 5 | Testes: unitários por transform + conservação por mídia | feito 30/09 |
| 6 | Registro em `TRANSFORMERS`/`PLATFORMS`, fora do catálogo | feito 30/09 |
| 7 | DAG `instagram_organic_daily` | feito 30/09 |
| 8 | Desenho implementado sobe para `docs/`; plano apagado | desenho em `docs/` feito 30/09; plano fica até fechar D2, D3 e D4 |

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
| ~~D1~~ | ~~**O `lifetime` perde o que tem mais de 2 anos?**~~ | **Fechada em 06/10:** o `reach` de conteúdo com 2+ anos é resíduo, sim — mas nenhum post caiu no próprio aniversário. As 727 quedas aconteceram **todas na foto de 02/10**, em lote. A regra por idade da D1 não entra; vira D3 |
| D2 | **Revisão tardia das métricas da conta.** O conector relê só o dia anterior e a Meta avisa revisão até 48h, então cada dia para de ser lido muito antes de parar de mudar | Medir por alguns dias a diferença entre a penúltima e a última leitura de cada dia. **Aberta, agora mensurável:** há 7 dias de fotos (29/09–05/10) e o cron principal não mudou. Ao medir, refazer a conta de quantas horas depois do fim do dia vem a última leitura — a versão anterior desta linha partia de um cron à 01:00, e o real é **02:00 São Paulo** |
| D3 | **Horizonte por métrica dentro do pipeline** (mesma do Facebook, ver `facebook-organic.md`). No Instagram o degrau está no `reach`: mediana 251 em 2024-10 contra 4.299 em 2024-11 | Decidir onde mora: `lifetime_truncated_at` no silver, detectado pela queda, e/ou horizonte por `platform` × métrica no gold, com "não medido" distinto de zero |
| ~~D4~~ | ~~**Hora do sync diário de stories.**~~ | **Fechada em 06/10:** cron em `0 30 9 * * ? America/Sao_Paulo`. Resolve os stories da manhã (lidos com ~23h); os da noite continuam lidos com ~16h, e fechar isso exige uma segunda leitura no fim da tarde, não uma hora mais cedo. Fica assim por ora — sem campanha, 16h de acumulação é aceitável |

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

**30/09** — **Gold reorganizada como camada de consumo, compartilhada com o Facebook.** A gold
antiga expunha a mecânica de construção (placar acumulado, `baseline_kind`, horas entre fotos)
e parecia bronze. Decisão:

- A série diária (total e delta por foto) passa a ser padronização e vai para o silver
  (`media_metrics_daily`), com as colunas de controle para auditoria.
- A gold vira `gold/organic/`, com as duas redes nas mesmas tabelas, nomes de negócio e
  `metrics_scope` (`organic` × `total`): `content` (dimensão do post, sem métrica),
  `content_daily` (cada métrica em par: o do dia e `_total` até o dia), `account_daily` e
  `stories`. Cada rede grava só a sua partição (`replaceWhere` em `platform`).
- O total atual de um post é o `_total` na última data do post; ele não pode vir da soma dos
  dias, que só conta a atividade desde o início do acompanhamento.

Implementado e rodado contra o MinIO de `192.168.10.80`: 2.681 mídias em `content`, 5.362 linhas
em `content_daily`, 155 dias de conta em `account_daily`, 12 stories. Testes: 54 orgânicos
verdes contra o remoto (conservação incluída); suíte inteira verde no local (73 passed, 6
skipped). As tabelas antigas continuam no MinIO e não são mais escritas:
`gold/instagram_organic/{media_daily_metrics,account_daily_metrics,story_metrics}` e
`silver/instagram_organic/media_insights_snapshot`.

**06/10** — **D1 medida nos 7 dias de fotos (29/09 a 05/10) e fechada.** Do
`silver/instagram_organic/media_metrics_daily`:

- O post-testemunha (`18153850864324974`, publicado 30/09/2024) caiu de **1.666 para 81 de
  alcance na foto de 02/10**, e ficou em 81. Completou 2 anos em 30/09 — dois dias antes da
  queda, com duas fotos (30/09 e 01/10) ainda em 1.666. E o post `17845158231310366`
  (05/10/2024) caiu no mesmo 02/10, com **23,9 meses** — antes do aniversário.
- `reach` teve **727 quedas, todas na foto de 02/10**, idades de 23 a 93 meses, e **nenhuma
  queda nas fotos de 03, 04 e 05/10**. Não é janela móvel por post: é um lote numa data só.
  Atingiu 676 dos 1.319 conteúdos com 24+ meses naquela foto; 127 já estavam zerados.
- `likes` e `comments` caem todo dia, de 1 a 7 unidades, em qualquer idade — é gente
  descurtindo, não retenção. Nenhuma relação com os 2 anos.
- O degrau real está no mês de publicação, e esse sim é compatível com ~24 meses: mediana de
  `reach` na foto de 05/10 é **251 em 2024-10 contra 4.299 em 2024-11**; antes de 2022 a
  mediana é 0 a 4, com curtidas em 60 a 180 — o retrato de "a Meta respondeu zero, não erro".

Conclusão igual à do Facebook: o corte existe, mas **o gatilho não é o aniversário do post**, e
por isso a regra por idade da D1 sai. O que dá para detectar é a queda (delta muito negativo
numa métrica de insight) e o horizonte por métrica. Vira D3.

**06/10** — **Cron dos stories: de horário para uma vez por dia, e depois para 09:30 São
Paulo.** Decisão do Pablo, para não multiplicar o raw por 24 em Append e porque não há campanha
exigindo acompanhamento horário. Medido no lake:

- A última leitura horária foi em **03/10 21:00 UTC**; a leitura diária seguinte veio em
  **06/10 06:00 UTC**. Nessas ~33 horas nenhum story foi lido: o que tenha sido publicado
  depois de 03/10 ~22:00 UTC expirou sem leitura e **não existe mais** — insight de story só
  vive 24h. Não há story com `created_at` em 03 ou 04/10 na tabela; se houve publicação nesses
  dias, foi perdida, e não há como recuperar.
- Na hora provisória (06:00 UTC = 03:00 São Paulo), o `hours_live_at_last_read` dos stories de
  05/10 ficou em **9,5 a 16,1 horas**, contra **23,0 a 24,0** em todos os dias de sync horário.
  As contas publicam ~11:00 e ~17:30 São Paulo, então a leitura das 03:00 pegava o story a meio
  da vida. Daí o cron final: **09:30 São Paulo** (`0 30 9 * * ?`), pouco antes da primeira
  janela de publicação — o story da manhã anterior é lido com ~23h e sobra mais de uma hora de
  margem. O da noite segue com ~16h; resolver isso pede uma segunda leitura no fim da tarde.
- **O rótulo de `snapshot_date` mudou de lado.** `SNAPSHOT_CUTOFF_HOURS` = 6: leitura antes das
  06:00 São Paulo fecha o dia anterior, leitura às 09:30 fecha o próprio dia. A data de negócio
  do gold vem de `published_date`, então consumidor nenhum sente; só uma auditoria foto a foto
  cruza a troca.
- **Stories passam a entrar no bronze com um dia de atraso**: o DAG roda às 08:00 UTC (05:00 São
  Paulo), antes das 09:30, então a leitura do dia D só é transformada na rodada de D+1. Nada se
  perde, mas o gold de stories fica um dia atrás do de conteúdo.
- O `hours_live_at_last_read` já está no silver e no `gold/organic/stories`, então o consumidor
  tem como saber que um story de 9,5h e um de 23h não se comparam.

**06/10** — **A listagem das conexões pela API do Airbyte contrariou a doc em dois pontos**
(`/api/public/v1/connections`, com as credenciais do `.env`):

- A conexão principal do Instagram roda às **02:00** São Paulo, não à 01:00 como a tabela do
  `airbyte/README.md` dizia. O diário de 30/09 já registrava "sync 36 (02:01 São Paulo)" — a
  tabela é que nasceu errada. Corrigida. A conta de horas da D2 partia do 01:00 e precisa ser
  refeita quando a decisão for medida.
- O comentário de `src/transformers/snapshots.py` ainda diz que "os crons do Airbyte rodam à
  01:00", o que agora é falso para quatro das sete conexões (Instagram principal 02:00,
  stories 09:30, Facebook Marketing 04:00, Kwai 06:00). O corte de 6 horas continua certo; a
  justificativa escrita é que envelheceu.
