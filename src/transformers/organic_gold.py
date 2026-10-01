"""Contrato da gold orgânica — as tabelas que Facebook e Instagram entregam juntas.

A gold é a camada de consumo: nomes de negócio, as duas redes na mesma tabela, nada da
mecânica de construção (placar acumulado, baseline, horas entre fotos — isso fica no silver).
Cada plataforma grava a sua fatia (`platform`) das mesmas tabelas em `gold/organic/`, com
`replaceWhere`, e por isso o schema tem que sair idêntico das duas: `conform` garante nome,
ordem e tipo, e coluna que a rede não tem vira nula.

| Tabela | Uma linha é |
|---|---|
| `content` | um post (atributos, sem métrica) |
| `content_daily` | um post num dia: o que aconteceu no dia e o total até o dia |
| `account_daily` | uma página ou conta num dia |
| `stories` | um story (só Instagram) |

`metrics_scope` diz se o número é `organic` (Instagram: a Meta exclui anúncios) ou `total`
(Facebook, e as contas das duas redes: inclui anúncios). Nunca some um com o outro.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, lit

GOLD_PREFIX = "organic"

ORGANIC = "organic"
TOTAL = "total"

# Métricas de conteúdo. Cada uma sai em par: o que aconteceu no dia e o total até o dia.
CONTENT_METRICS: tuple[str, ...] = (
    "views",
    "reach",
    "likes",
    "comments",
    "shares",
    "saves",
    "clicks",
    "follows",
    "profile_visits",
)


def daily_name(metric: str) -> str:
    """O alcance do dia é gente **nova** alcançada — não o alcance do dia."""
    return "reach_new" if metric == "reach" else metric


def total_name(metric: str) -> str:
    return f"{metric}_total"


CONTENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("platform", "string"),
    ("content_id", "string"),
    ("account_id", "string"),
    ("account_name", "string"),
    ("format", "string"),
    ("published_at", "timestamp"),
    ("published_date", "date"),
    ("caption", "string"),
    ("permalink", "string"),
    ("last_seen_date", "date"),
    ("metrics_scope", "string"),
)

CONTENT_DAILY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("date", "date"),
    ("platform", "string"),
    ("account_id", "string"),
    ("content_id", "string"),
    ("metrics_scope", "string"),
    ("is_first_tracked_day", "boolean"),
    ("days_covered", "int"),
    *(
        (name, "bigint")
        for metric in CONTENT_METRICS
        for name in (daily_name(metric), total_name(metric))
    ),
)

ACCOUNT_DAILY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("date", "date"),
    ("platform", "string"),
    ("account_id", "string"),
    ("account_name", "string"),
    ("metrics_scope", "string"),
    ("is_partial", "boolean"),
    ("views", "bigint"),
    ("reach", "bigint"),
    ("post_engagements", "bigint"),
    ("new_followers", "bigint"),
    ("new_followers_organic", "bigint"),
    ("followers_total", "bigint"),
)

STORY_COLUMNS: tuple[tuple[str, str], ...] = (
    ("platform", "string"),
    ("story_id", "string"),
    ("account_id", "string"),
    ("account_name", "string"),
    ("format", "string"),
    ("published_at", "timestamp"),
    ("published_date", "date"),
    ("permalink", "string"),
    ("metrics_scope", "string"),
    ("reach", "bigint"),
    ("views", "bigint"),
    ("shares", "bigint"),
    ("follows", "bigint"),
    ("replies", "bigint"),
    ("profile_visits", "bigint"),
    ("interactions", "bigint"),
    ("last_read_at", "timestamp"),
    ("hours_live_at_last_read", "double"),
)

COLUMNS_BY_TABLE: dict[str, tuple[tuple[str, str], ...]] = {
    "content": CONTENT_COLUMNS,
    "content_daily": CONTENT_DAILY_COLUMNS,
    "account_daily": ACCOUNT_DAILY_COLUMNS,
    "stories": STORY_COLUMNS,
}

GRAIN_BY_TABLE: dict[str, tuple[str, ...]] = {
    "content": ("platform", "content_id"),
    "content_daily": ("platform", "content_id", "date"),
    "account_daily": ("platform", "account_id", "date"),
    "stories": ("platform", "story_id"),
}


def conform(df: DataFrame, table: str, platform: str, **constants: Column) -> DataFrame:
    """Coloca o DataFrame no schema da tabela: nome, ordem e tipo. `platform` e as
    `constants` (ex.: `metrics_scope`) entram como valor fixo; coluna ausente vira nula."""
    fixed = {"platform": lit(platform), **constants}
    return df.select(
        *(
            (fixed[name] if name in fixed else col(name) if name in df.columns else lit(None))
            .cast(dtype)
            .alias(name)
            for name, dtype in COLUMNS_BY_TABLE[table]
        )
    )


def content_daily_pairs(metric_sources: dict[str, str]) -> list[Column]:
    """Pares dia/total do `content_daily` a partir das colunas `{m}_delta`/`{m}_lifetime` do
    silver. `metric_sources` mapeia a métrica da gold → a métrica do silver da plataforma;
    métrica ausente do mapa fica nula (a rede não tem)."""
    columns: list[Column] = []
    for metric in CONTENT_METRICS:
        source = metric_sources.get(metric)
        if source is None:
            continue
        columns.append(col(f"{source}_delta").alias(daily_name(metric)))
        columns.append(col(f"{source}_lifetime").alias(total_name(metric)))
    return columns


def series_flags() -> list[Column]:
    """`is_first_tracked_day`: primeira foto do post — o dia só é conhecido se o post nasceu
    nela. `days_covered`: quantos dias o número do dia cobre (mais de 1 depois de um sync que
    faltou)."""
    return [
        col("prev_snapshot_date").isNull().alias("is_first_tracked_day"),
        col("gap_days").alias("days_covered"),
    ]
