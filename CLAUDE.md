# binder_etl

ETL interno: extrai dados de plataformas de mídia via Airbyte, transforma em camadas medallion
no MinIO com Spark + Delta, e expõe uma FastAPI de catálogo para o backend descobrir
identidades do lake.

**Fluxo:** plataformas → Airbyte → `raw` → Spark `bronze`/`silver`/`gold` → o backend
consulta o catálogo para configurar o Bridge → o DAG busca a publicação de enriquecimento e
materializa o **gold enriquecido**.

## Iniciativa ativa

`bridge-enrichment` — passos, próxima ação e decisões em aberto em
[docs/plans/bridge-enrichment.md](docs/plans/bridge-enrichment.md). Plano único dos três
repositórios.

`organic` — conteúdo orgânico de páginas do Facebook e contas de Instagram em
bronze/silver/gold, e a orquestração que garante a gold do dia, só no ETL, em
[docs/plans/organic.md](docs/plans/organic.md). Uma iniciativa, não duas: as duas redes
compartilham `gold/organic/`, o `snapshots.py` e os DAGs.

## Arquitetura de enriquecimento (invariantes compartilhadas)

Valem nos três repositórios. Contradizer uma delas é bug, não escolha de implementação.

1. **Um fato: ad × dia.** `campaign`, `ad_group` e `ad` são níveis de *declaração*, não grãos
   de dado. Tudo resolve até a linha ad × dia.
2. **Um atributo, um nível.** Cada atributo é declarado em exatamente um nível e propaga para
   baixo. Sem override, sem declaração dupla — por isso não existe `coalesce` nem precedência.
3. **O nível é do negócio, não da plataforma.** Onde a plataforma guarda um dado é irrelevante.
4. **A amarração é do banco.** Integridade vira constraint no Postgres do backend, não
   validação imperativa.
5. **Nada some por enriquecimento.** Soma sem filtro no fato enriquecido é idêntica à soma no
   fato cru. Todo join é `LEFT`; ausência vira categoria explícita.
6. **SCD tipo 1.** A verdade é a configuração atual; corrigir reescreve o histórico. Por isso
   o `Overwrite` full do gold enriquecido é a semântica **correta**, não uma limitação.

| Atributo | Declarado em | Propaga para | Origem |
|---|---|---|---|
| Cliente | account | tudo abaixo | configuração |
| Campanha de negócio | campaign | ad_group, ad | configuração |
| Channel | campaign | ad_group, ad | configuração manual |
| Buying type | campaign | ad_group, ad | plano de mídia |
| Território, Persona, … | ad_group | ad | configuração |
| Format / Sub-format | ad | — | traduzido do nativo |

**Identificação e classificação.** A tabela acima tem dois tipos de linha. *Identificação* diz
a que entidade de negócio o objeto pertence — conta → cliente, campanha da plataforma →
campanha de negócio. É declarada uma vez e herdada por toda a hierarquia abaixo; nenhum nível
abaixo a digita. *Classificação* anexa atributos: channel, buying type, eixos, formato. O
vocabulário disponível para classificar um nível é limitado pelo escopo que a identificação de
cima estabeleceu — os eixos de um ad_group são os da campanha de negócio do binding dele, e
não outros. Por isso o nível de ad_group só classifica: a identificação ele herda.

> Este bloco é espelhado em `binder_app_backend/CLAUDE.md` e `binder_app_frontend/CLAUDE.md`.
> Ao mudar, mude nos três.

## Camadas medallion

| Camada | Caminho | Formato | Responsabilidade |
|---|---|---|---|
| Raw | `raw/airbyte/{platform}/{stream}/` | Parquet | Landing do Airbyte — nunca editar à mão. Acumula um arquivo por sync |
| Bronze | `bronze/{platform}/{table}/` | Delta | Camada acumuladora: union com o existente + dedupe pela chave natural |
| Silver | `silver/{platform}/{table}/` | Delta | Renomes e tipos corretos; o fato descarta ad × dia sem nenhuma métrica |
| Gold | `gold/{platform}/{fact}/` | Delta | Fatos no grão de negócio. As orgânicas compartilham `gold/organic/{tabela}/` |

**Existência de objeto vem do status, não de `_airbyte_extracted_at`.** No incremental, um
objeto só é reextraído quando muda, então um `extracted_at` antigo não significa que ele
sumiu. Deletados chegam explicitamente com `*_STATUS_DELETE` em `secondary_status`.

## Regras duras

- **Não escrever linhas do Bridge a partir daqui.** O Bridge é SSOT do backend.
- **O backend nunca faz I/O no MinIO.** I/O de lake fica no ETL; o backend consulta por HTTP.
- **Cast de IDs de plataforma para `string`** no silver — casa com `varchar(255)` do Bridge.
- **Preservar colunas de linhagem** do Airbyte: `_airbyte_raw_id`, `_airbyte_extracted_at`,
  `_airbyte_meta`.
- **Não guardar entidades de negócio do Binder no MinIO** — isso vive no Postgres.
- **Chave de dedupe é a chave natural mínima** (`ad_id`, não `advertiser_id + campaign_id +
  adgroup_id + ad_id`). O raw acumula versões; chave composta demais faz um objeto que mudou
  de pai virar duas linhas e duplica métrica.
- **O fato não traz id de conta.** `advertiser_id` existe no schema do `ads_reports_daily`
  mas vem `NULL` do conector. `ad_account_id` deriva da hierarquia, pela dimensão `ads`.
- **Todo join do gold é `LEFT`, partindo do fato.** A extração nunca é garantidamente
  completa; o join não pode depender dela.
- **O join de enriquecimento casa pelo id do objeto, nunca pela conta.** `campaign_id`,
  `ad_group_id` e `ad_id` vêm do fato e estão sempre presentes; `ad_account_id` é derivado da
  dimensão `ads` e pode faltar — e `NULL` não casa com `NULL`. O snapshot é filtrado por
  plataforma antes do join. Quem garante que não há dois candidatos para o mesmo id é a
  unicidade por coordenada externa no Bridge, não o ETL.
- **Linha não é dinheiro.** Meça conservação por soma de métrica, não por contagem de linhas.
- **Mudar `start_date` ou *Include Deleted* no Airbyte exige Clear data dos streams.**
- **Sessão Spark em UTC** (`spark_session.py`). `to_date`, `from_utc_timestamp` e casts de
  string para timestamp dependem dela; os containers já rodam em UTC, a máquina de dev não.
- **Plataformas orgânicas guardam uma foto por dia.** No `facebook_organic` e no
  `instagram_organic`, o bronze dos streams full refresh deduplica por `id + snapshot_date` — é
  a chave natural mínima de uma fonte que só entrega total acumulado. Deduplicar só pelo id
  apagaria o histórico. Raw lido com schema explícito (`tables.py`), nunca inferido.
- **Gold orgânica: cada rede grava só a sua partição.** Facebook e Instagram escrevem nas
  mesmas tabelas de `gold/organic/`, com `replace_where` na coluna `platform`. Overwrite da
  tabela inteira apagaria a outra rede. A gold é de consumo — a mecânica do delta fica no silver.
- **Conexões do Airbyte sempre em Append, nunca Overwrite.** Overwrite apaga os arquivos
  anteriores do stream a cada sync; o bronze só acumula o que ainda está no raw quando roda.
- **Stories do Instagram: nunca deixar 24h entre duas leituras.** O insight de story só existe
  enquanto o story está vivo; publicado depois de uma leitura e expirado antes da próxima, ele
  não chega ao lake e não há como recuperar. Trocar o cron abre exatamente esse buraco — a
  troca anterior custou ~33h sem leitura. Story se compara por `hours_live_at_last_read`, não
  pelo número cru: lido com 9h, ele ainda estava acumulando.
- **Quem dispara a extração é dono do horário.** Conexão do Airbyte que um DAG dispara fica em
  `scheduleType: "manual"` — com o cron vivo, o disparo cai em cima do job do cron e o DAG
  espera um job que não é o dele. "Manual" é o schedule, não o `status`: `inactive` desliga a
  conexão até para a API.
- **Fan-out de conexão é da extração; o medallion é um por plataforma.** O raw do Facebook é
  por página e o transformer lê com curinga, então N syncs em paralelo e **um** medallion
  depois. N medallions reprocessariam todas as páginas e disputariam a mesma partição da gold.
  Medallion roda serializado (pool de um slot): `local[*]` em quatro núcleos não divide.
- **Conexão de página nova nasce com `namespaceFormat = facebook_organic/{page_id}`.** Com o
  namespace default os arquivos caem fora do curinga e a página desaparece do lake sem erro.
  Conexão orgânica leva a tag `Organic` — é por ela que o DAG do Facebook descobre as páginas.
- **Métrica de insight de conteúdo antigo é resíduo, não zero real.** Fora da janela de
  retenção a Meta responde um número minúsculo em vez de erro, o corte não é o aniversário do
  conteúdo e cada métrica morre numa idade diferente. Derive o horizonte por plataforma **e por
  métrica** e marque o que está fora como não medido.
- **Instalar o Airbyte com as flags montadas do `.env`, nunca o `abctl local install` cru.**
  Sem `--insecure-cookies` o login quebra: o Airbyte é servido em HTTP puro e o navegador
  descarta o cookie de sessão marcado `Secure` — a credencial correta falha sem mensagem de
  erro. `AIRBYTE_HOST` vazio mantém o ingress curinga, necessário para acessar por IP na VPN.
  Reinstalar é `helm upgrade` e preserva conexões e estado; `uninstall --persisted` destrói.
  Comando e detalhes em [airbyte/README.md](airbyte/README.md).
- A FastAPI de catálogo precisa inicializar o `SparkSession` no lifespan **antes** de aceitar
  tráfego. Trabalho Spark/MinIO é síncrono — rode fora do event loop.

## Comandos

```bash
python -m src.pipelines.run medallion tiktok    # bronze → silver → gold
python -m src.pipelines.run bronze tiktok       # camada isolada
python -m src.pipelines.run medallion facebook_organic  # conteúdo das páginas do Facebook
python -m src.pipelines.run medallion instagram_organic # conteúdo das contas de Instagram
docker compose up                               # MinIO + Postgres (meta Airflow) + Airflow
docker compose --profile dev up spark-dev       # sandbox Spark
uvicorn src.api.main:app                        # FastAPI de catálogo
docker compose --profile dev run --rm spark-dev pytest tests/  # suíte no container (Spark + MinIO)
pytest tests/                                   # suíte inteira, todas as plataformas
pytest tests/transformers/tiktok/               # só TikTok — árvore espelha src/transformers/tiktok/
pytest tests/transformers/tiktok/test_conservation.py  # invariante de conservação (rode o medallion antes)
pytest tests/transformers/facebook_organic/     # só facebook_organic (conservação por post incluída)
pytest tests/transformers/instagram_organic/    # só instagram_organic (conservação por mídia incluída)
```

## Documentação

| Arquivo | Quando ler |
|---|---|
| [docs/architecture.md](docs/architecture.md) | extração, acumulação, gold base e enriquecido, conservação, publicação, Facebook e Instagram orgânicos |
| [docs/project-structure.md](docs/project-structure.md) | árvore de diretórios, convenções de nome, split layer/transform, testes |
| [docs/tech-stack.md](docs/tech-stack.md) | versões e práticas por biblioteca |
| [docs/plans/](docs/plans/) | trabalho em andamento: iniciativa ativa e backlog |

Skill `add-platform` (`.claude/skills/add-platform/`) para integrar uma plataforma nova.

## Onde cada informação mora (compartilhado)

| Tipo | Onde |
|---|---|
| Regra que vale sempre | `CLAUDE.md` |
| Como e por que funciona; desenho decidido | `docs/*.md` — no presente, sem data, sem número de passo, volumes em ordem de grandeza |
| O que falta, status, decisões em aberto, medições datadas | `docs/plans/<iniciativa>.md` |
| Ideia ainda sem escopo | `docs/plans/backlog.md` |

Iniciativa que envolve mais de um repositório tem um plano só, no repositório onde começou;
os outros apontam para ele.

Todo passo de uma iniciativa termina com: testes verdes → status e diário atualizados no
plano → regra nova sobe para o `CLAUDE.md` e mudança de desenho para `docs/` → rótulos
"alvo"/"legado" que ficaram falsos saem → a checagem abaixo volta vazia. Ao encerrar a
iniciativa, o plano é apagado e o ponteiro sai do `CLAUDE.md`.

```bash
grep -rnE "\bpasso [0-9]|\bfeito\b|[0-9]{2}/[0-9]{2}/20[0-9]{2}" CLAUDE.md docs .claude --exclude-dir=plans 2>/dev/null
```

> Este bloco é espelhado nos três repositórios. Ao mudar, mude nos três.
