"""Unitário e isolado (sem MinIO): a fatia do Facebook na gold orgânica.

A gold é a camada de consumo: nomes de negócio e o mesmo schema do Instagram
(`src/transformers/organic_gold.py`). Os testes provam o mapeamento das métricas do Facebook
para as colunas comuns e que o schema sai exatamente o do contrato.
"""
from datetime import date, datetime, timezone

from src.transformers.facebook_organic.tables import GOLD_FACTS
from src.transformers.facebook_organic.transforms.gold import GOLD_TRANSFORMS
from src.transformers.organic_gold import COLUMNS_BY_TABLE

FACTS = {fact.name: fact for fact in GOLD_FACTS}
PAGES = [("p1", "Texaco Lubrificantes")]
PAGES_SCHEMA = "page_id string, page_name string"


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _run(spark, table, sources):
    frames = {name: spark.createDataFrame(rows, schema=schema) for name, (rows, schema) in sources.items()}
    df = GOLD_TRANSFORMS[table](frames, FACTS[table])
    assert df.dtypes == list(COLUMNS_BY_TABLE[table]), "schema fora do contrato"
    return df.collect()


def test_content_maps_post_attributes(spark):
    rows = _run(
        spark,
        "content",
        {
            "posts": (
                [("p1_x", "p1", _utc(2026, 9, 29, 14, 50), date(2026, 9, 29), "Mais de um século",
                  "https://facebook.com/x", "photo", date(2026, 9, 30))],
                "post_id string, page_id string, created_at timestamp, created_date date, "
                "message string, permalink_url string, media_type string, last_seen_date date",
            ),
            "pages": (PAGES, PAGES_SCHEMA),
        },
    )

    row = rows[0]
    assert (row["platform"], row["content_id"], row["account_name"]) == (
        "facebook", "p1_x", "Texaco Lubrificantes",
    )
    assert row["format"] == "image"  # `photo` do Facebook = `image` do Instagram
    assert row["caption"] == "Mais de um século"
    assert row["metrics_scope"] == "total"


def test_content_daily_pairs_and_flags(spark):
    series_schema = (
        "post_id string, page_id string, snapshot_date date, prev_snapshot_date date, "
        "gap_days int, media_views_lifetime bigint, media_views_delta bigint, "
        "reach_lifetime bigint, reach_delta bigint, reactions_total_lifetime bigint, "
        "reactions_total_delta bigint, shares_lifetime bigint, shares_delta bigint, "
        "clicks_lifetime bigint, clicks_delta bigint"
    )
    rows = _run(
        spark,
        "content_daily",
        {
            "post_metrics_daily": (
                [
                    ("p1_x", "p1", date(2026, 9, 29), None, None, 2006, None, 1906, None, 3, None, 0, None, 1, None),
                    ("p1_x", "p1", date(2026, 9, 30), date(2026, 9, 29), 1, 2011, 5, 1911, 5, 4, 1, 1, 1, 1, 0),
                ],
                series_schema,
            )
        },
    )
    by_day = {row["date"]: row for row in rows}

    first, second = by_day[date(2026, 9, 29)], by_day[date(2026, 9, 30)]
    assert first["is_first_tracked_day"] is True
    assert first["views"] is None and first["views_total"] == 2006  # dia desconhecido, total conhecido
    assert second["is_first_tracked_day"] is False
    assert second["days_covered"] == 1
    assert (second["views"], second["views_total"]) == (5, 2011)
    assert (second["reach_new"], second["reach_total"]) == (5, 1911)
    assert (second["likes"], second["likes_total"]) == (1, 4)  # likes = todas as reações
    assert second["comments_total"] is None  # o Facebook não entrega comentários por post
    assert second["metrics_scope"] == "total"


def test_account_daily_pivots_page_metrics(spark):
    rows = _run(
        spark,
        "account_daily",
        {
            "page_insights_daily": (
                [
                    ("p1", "page_media_view", "day", date(2026, 9, 28), 483992, None),
                    ("p1", "page_media_view", "week", date(2026, 9, 28), 1, None),
                    ("p1", "page_total_media_view_unique", "day", date(2026, 9, 28), 271922, None),
                    ("p1", "page_post_engagements", "day", date(2026, 9, 28), 665, None),
                    (
                        "p1", "page_fan_adds_by_paid_non_paid_unique", "day", date(2026, 9, 28),
                        None, {"total": 8, "paid": 7, "unpaid": 1},
                    ),
                ],
                "page_id string, metric string, period string, metric_date date, value bigint, "
                "value_breakdown map<string, bigint>",
            ),
            "page_followers_snapshot": (
                [("p1", date(2026, 9, 28), 1783)],
                "page_id string, snapshot_date date, followers_count bigint",
            ),
            "pages": (PAGES, PAGES_SCHEMA),
        },
    )

    row = rows[0]
    assert (row["views"], row["reach"], row["post_engagements"]) == (483992, 271922, 665)
    assert (row["new_followers"], row["new_followers_organic"]) == (8, 1)
    assert row["followers_total"] == 1783
    assert row["is_partial"] is False
    assert row["metrics_scope"] == "total"
