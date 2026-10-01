"""Unitário e isolado (sem MinIO): a fatia do Instagram na gold orgânica.

Mesmo contrato do Facebook (`src/transformers/organic_gold.py`). Os testes provam o mapeamento
das métricas do Instagram para as colunas comuns, que métrica não pedida continua nula, e que o
schema sai exatamente o do contrato.
"""
from datetime import date, datetime, timezone

from src.transformers.instagram_organic.tables import GOLD_FACTS
from src.transformers.instagram_organic.transforms.gold import GOLD_TRANSFORMS
from src.transformers.organic_gold import COLUMNS_BY_TABLE

FACTS = {fact.name: fact for fact in GOLD_FACTS}
ACCOUNTS = [("acc", "texacolubrificantes")]
ACCOUNTS_SCHEMA = "business_account_id string, username string"


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _run(spark, table, sources):
    frames = {name: spark.createDataFrame(rows, schema=schema) for name, (rows, schema) in sources.items()}
    df = GOLD_TRANSFORMS[table](frames, FACTS[table])
    assert df.dtypes == list(COLUMNS_BY_TABLE[table]), "schema fora do contrato"
    return df.collect()


def test_content_uses_username_and_is_organic(spark):
    rows = _run(
        spark,
        "content",
        {
            "media": (
                [("m1", "acc", "texacolubrificantes", _utc(2026, 9, 11, 21, 18), date(2026, 9, 11),
                  "carousel", "Potência e nostalgia", "https://instagram.com/p/x", date(2026, 9, 30))],
                "media_id string, business_account_id string, username string, created_at timestamp, "
                "created_date date, format string, caption string, permalink string, last_seen_date date",
            ),
            "accounts": (ACCOUNTS, ACCOUNTS_SCHEMA),
        },
    )

    row = rows[0]
    assert (row["platform"], row["account_name"], row["format"]) == (
        "instagram", "texacolubrificantes", "carousel",
    )
    assert row["metrics_scope"] == "organic"


def test_content_daily_maps_metrics_and_keeps_nulls(spark):
    metrics = ("likes", "comments", "reach", "views", "saved", "shares", "follows", "profile_visits")
    series_schema = (
        "media_id string, business_account_id string, snapshot_date date, "
        "prev_snapshot_date date, gap_days int, "
        + ", ".join(f"{m}_lifetime bigint, {m}_delta bigint" for m in metrics)
    )
    # carrossel: sem views (não pedido)
    values = {
        "likes": (200, 10), "comments": (5, 0), "reach": (3159, 59), "views": (None, None),
        "saved": (13, 1), "shares": (20, 0), "follows": (3, 0), "profile_visits": (15, 1),
    }
    row_values = [v for m in metrics for v in values[m]]
    rows = _run(
        spark,
        "content_daily",
        {
            "media_metrics_daily": (
                [("m1", "acc", date(2026, 9, 30), date(2026, 9, 29), 1, *row_values)],
                series_schema,
            )
        },
    )

    row = rows[0]
    assert (row["likes"], row["likes_total"]) == (10, 200)
    assert (row["saves"], row["saves_total"]) == (1, 13)  # `saved` do Instagram
    assert (row["reach_new"], row["reach_total"]) == (59, 3159)
    assert row["views"] is None and row["views_total"] is None  # carrossel: nulo, não 0
    assert row["clicks_total"] is None  # o Instagram não entrega cliques por mídia
    assert row["metrics_scope"] == "organic"


def test_account_daily_keeps_partial_flag(spark):
    rows = _run(
        spark,
        "account_daily",
        {
            "account_insights_daily": (
                [("acc", date(2026, 9, 30), True, 27980, 0)],
                "business_account_id string, metric_date date, is_partial boolean, "
                "reach bigint, new_followers bigint",
            ),
            "account_followers_snapshot": (
                [("acc", date(2026, 9, 30), 140958)],
                "business_account_id string, snapshot_date date, followers_count bigint",
            ),
            "accounts": (ACCOUNTS, ACCOUNTS_SCHEMA),
        },
    )

    row = rows[0]
    assert (row["reach"], row["is_partial"], row["followers_total"]) == (27980, True, 140958)
    assert row["views"] is None  # o conector não pede visualizações da conta
    assert row["metrics_scope"] == "total"  # o alcance da conta inclui anúncios


def test_stories_business_names(spark):
    rows = _run(
        spark,
        "stories",
        {
            "stories": (
                [("s1", "acc", _utc(2026, 9, 30, 13, 39), date(2026, 9, 30), "VIDEO", "p",
                  3274, 4041, 7, 0, 4, 13, 35, _utc(2026, 9, 30, 19, 0), 5.36)],
                "story_id string, business_account_id string, created_at timestamp, "
                "created_date date, media_type string, permalink string, reach bigint, "
                "views bigint, shares bigint, follows bigint, replies bigint, "
                "profile_visits bigint, total_interactions bigint, last_read_at timestamp, "
                "hours_live_at_last_read double",
            ),
            "accounts": (ACCOUNTS, ACCOUNTS_SCHEMA),
        },
    )

    row = rows[0]
    assert (row["format"], row["interactions"], row["account_name"]) == ("video", 35, "texacolubrificantes")
