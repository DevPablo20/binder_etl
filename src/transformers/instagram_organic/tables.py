"""Single source of truth for Instagram (organic) table metadata.

Um token enxerga todas as contas: o raw fica em `airbyte/instagram_organic/{stream}/`, sem pasta
por conta — todo registro traz `business_account_id`. Duas conexões escrevem ali: a principal
(diária) e a de stories (de hora em hora; métrica de story só existe enquanto o story está no
ar).

Os streams full refresh guardam uma foto por dia no bronze (`id + snapshot_date`); o
`user_insights` já é uma série diária e deduplica pela própria data; stories guardam só a
última leitura.
"""
from dataclasses import dataclass

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transformers.organic_gold import GOLD_PREFIX

RAW_PREFIX = "airbyte/instagram_organic"
PLATFORM = "instagram_organic"
# Valor da coluna `platform` nas tabelas da gold orgânica.
GOLD_PLATFORM = "instagram"

# O `date` do `user_insights` é a meia-noite do Pacífico que *inicia* o dia medido — o
# contrário do `end_time` das páginas do Facebook. Conferido contra o Business Suite.
INSIGHTS_TIMEZONE = "America/Los_Angeles"


_AIRBYTE_META = StructType(
    [
        StructField("sync_id", LongType()),
        StructField(
            "changes",
            ArrayType(
                StructType(
                    [
                        StructField("field", StringType()),
                        StructField("change", StringType()),
                        StructField("reason", StringType()),
                    ]
                )
            ),
        ),
    ]
)

_AIRBYTE_COLUMNS = [
    StructField("_airbyte_raw_id", StringType()),
    StructField("_airbyte_extracted_at", TimestampType()),
    StructField("_airbyte_meta", _AIRBYTE_META),
    StructField("_airbyte_generation_id", LongType()),
]

_ACCOUNT_COLUMNS = [
    StructField("business_account_id", StringType()),
    StructField("page_id", StringType()),
]


def _schema(*fields: StructField) -> StructType:
    return StructType([*_AIRBYTE_COLUMNS, *_ACCOUNT_COLUMNS, *fields])


# Schemas explícitos, lidos com `spark.read.schema(...)`. Sem eles, o Spark infere o schema de
# um único arquivo do raw e, se escolher um anterior à seleção de campos, colunas novas somem
# sem erro. Coluna ausente num arquivo vira NULL; coluna extra é ignorada.
USERS_SCHEMA = _schema(
    StructField("id", StringType()),
    StructField("username", StringType()),
    StructField("name", StringType()),
    StructField("followers_count", LongType()),
    StructField("follows_count", LongType()),
    StructField("media_count", LongType()),
)

USER_INSIGHTS_SCHEMA = _schema(
    StructField("date", TimestampType()),
    StructField("reach", LongType()),
    StructField("reach_week", LongType()),
    StructField("reach_days_28", LongType()),
    StructField("follower_count", LongType()),
    StructField("online_followers", StringType()),
)

USER_LIFETIME_INSIGHTS_SCHEMA = _schema(
    StructField("metric", StringType()),
    StructField("breakdown", StringType()),
    StructField("value", StringType()),
)

MEDIA_SCHEMA = _schema(
    StructField("id", StringType()),
    StructField("ig_id", StringType()),
    StructField("username", StringType()),
    StructField("caption", StringType()),
    StructField("permalink", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("media_type", StringType()),
    StructField("media_product_type", StringType()),
    StructField("like_count", LongType()),
    StructField("comments_count", LongType()),
    StructField("is_comment_enabled", BooleanType()),
    StructField("thumbnail_url", StringType()),
)

MEDIA_INSIGHTS_SCHEMA = _schema(
    StructField("id", StringType()),
    StructField("likes", LongType()),
    StructField("comments", LongType()),
    StructField("reach", LongType()),
    StructField("views", LongType()),
    StructField("saved", LongType()),
    StructField("shares", LongType()),
    StructField("follows", LongType()),
    StructField("profile_visits", LongType()),
    StructField("ig_reels_avg_watch_time", DoubleType()),
    StructField("ig_reels_video_view_total_time", DoubleType()),
)

STORIES_SCHEMA = _schema(
    StructField("id", StringType()),
    StructField("caption", StringType()),
    StructField("permalink", StringType()),
    StructField("shortcode", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("media_type", StringType()),
    StructField("media_product_type", StringType()),
    StructField("thumbnail_url", StringType()),
)

STORY_INSIGHTS_SCHEMA = _schema(
    StructField("id", StringType()),
    StructField("reach", LongType()),
    StructField("views", LongType()),
    StructField("shares", LongType()),
    StructField("follows", LongType()),
    StructField("replies", LongType()),
    StructField("profile_visits", LongType()),
    StructField("total_interactions", LongType()),
)


@dataclass(frozen=True)
class InstagramStream:
    name: str
    schema: StructType
    dedupe_columns: tuple[str, ...]
    # Linhas que não são dado: descartadas antes do dedupe.
    required_column: str | None = None

    @property
    def raw_path(self) -> str:
        return f"{RAW_PREFIX}/{self.name}"

    @property
    def bronze_table_name(self) -> str:
        return f"{PLATFORM}/{self.name}"


@dataclass(frozen=True)
class SilverTableConfig:
    name: str
    bronze_sources: tuple[str, ...]

    @property
    def silver_table_name(self) -> str:
        return f"{PLATFORM}/{self.name}"


@dataclass(frozen=True)
class GoldFactConfig:
    name: str
    silver_sources: tuple[str, ...]

    @property
    def gold_table_name(self) -> str:
        """A gold orgânica é compartilhada com o Facebook: `gold/organic/{tabela}`."""
        return f"{GOLD_PREFIX}/{self.name}"

    @property
    def silver_source_paths(self) -> list[str]:
        return [f"{PLATFORM}/{source}" for source in self.silver_sources]


SNAPSHOT_KEY = ("id", "snapshot_date")

INSTAGRAM_STREAMS: tuple[InstagramStream, ...] = (
    InstagramStream(name="users", schema=USERS_SCHEMA, dedupe_columns=SNAPSHOT_KEY),
    InstagramStream(
        name="user_lifetime_insights",
        schema=USER_LIFETIME_INSIGHTS_SCHEMA,
        dedupe_columns=("business_account_id", "metric", "breakdown", "snapshot_date"),
    ),
    InstagramStream(name="media", schema=MEDIA_SCHEMA, dedupe_columns=SNAPSHOT_KEY),
    InstagramStream(
        name="media_insights", schema=MEDIA_INSIGHTS_SCHEMA, dedupe_columns=SNAPSHOT_KEY
    ),
    # Série nativa: o dia é a chave, e a releitura do dia seguinte fecha o dia em andamento.
    # O primeiro sync trouxe linhas sem `date` (só ids) — não são dado.
    InstagramStream(
        name="user_insights",
        schema=USER_INSIGHTS_SCHEMA,
        dedupe_columns=("business_account_id", "date"),
        required_column="date",
    ),
    # Só a última leitura de cada story: é a final, lida até 1h antes de o story expirar.
    InstagramStream(name="stories", schema=STORIES_SCHEMA, dedupe_columns=("id",)),
    InstagramStream(
        name="story_insights", schema=STORY_INSIGHTS_SCHEMA, dedupe_columns=("id",)
    ),
)

SILVER_TABLES: tuple[SilverTableConfig, ...] = (
    SilverTableConfig(name="accounts", bronze_sources=("users",)),
    SilverTableConfig(name="account_followers_snapshot", bronze_sources=("users",)),
    SilverTableConfig(name="account_insights_daily", bronze_sources=("user_insights",)),
    SilverTableConfig(
        name="follower_demographics_snapshot", bronze_sources=("user_lifetime_insights",)
    ),
    SilverTableConfig(name="media", bronze_sources=("media",)),
    SilverTableConfig(name="media_metrics_daily", bronze_sources=("media_insights", "media")),
    SilverTableConfig(name="stories", bronze_sources=("stories", "story_insights")),
)

GOLD_FACTS: tuple[GoldFactConfig, ...] = (
    GoldFactConfig(name="content", silver_sources=("media", "accounts")),
    GoldFactConfig(name="content_daily", silver_sources=("media_metrics_daily",)),
    GoldFactConfig(
        name="account_daily",
        silver_sources=("account_insights_daily", "account_followers_snapshot", "accounts"),
    ),
    GoldFactConfig(name="stories", silver_sources=("stories", "accounts")),
)

# Métricas de mídia do `/insights`: nome no raw → coluna `_lifetime`. O conector pede um
# conjunto por tipo de mídia (fixo no manifest dele); a métrica não pedida vem nula e continua
# nula — nunca vira 0.
MEDIA_INSIGHT_METRICS: dict[str, str] = {
    "reach": "reach",
    "views": "views",
    "saved": "saved",
    "shares": "shares",
    "follows": "follows",
    "profile_visits": "profile_visits",
    "ig_reels_video_view_total_time": "reels_view_total_time",
}
