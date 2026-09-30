"""Single source of truth for Facebook Pages (organic) table metadata.

Uma conexão do Airbyte por página, todas no mesmo prefixo raw:
`airbyte/facebook_organic/{page_id}/{stream}/`. Os quatro streams são Full Refresh + Append —
cada sync é uma foto completa. O bronze guarda uma foto por dia de todos eles (chave
`id + snapshot_date`); o silver decide quem é dimensão (última foto) e quem é série.
"""
from dataclasses import dataclass

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_PREFIX = "airbyte/facebook_organic"
PLATFORM = "facebook_organic"

# O `end_time` dos insights de período `day` é a meia-noite do Pacífico que *encerra* o dia
# medido — confirmado contra o Business Suite.
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


def _insights_values(*value_fields: StructField) -> StructField:
    return StructField(
        "values",
        ArrayType(
            StructType(
                [
                    StructField("value", StructType(list(value_fields))),
                    StructField("end_time", TimestampType()),
                ]
            )
        ),
    )


# Schemas explícitos, lidos com `spark.read.schema(...)`. Sem eles, o Spark infere o schema de
# um único arquivo do raw e, se escolher um anterior à seleção de campos, colunas novas somem
# sem erro. Coluna ausente num arquivo vira NULL; coluna extra é ignorada.
PAGE_SCHEMA = StructType(
    [
        *_AIRBYTE_COLUMNS,
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("username", StringType()),
        StructField("link", StringType()),
        StructField("category", StringType()),
        StructField("fan_count", LongType()),
        StructField("followers_count", LongType()),
    ]
)

POST_SCHEMA = StructType(
    [
        *_AIRBYTE_COLUMNS,
        StructField("id", StringType()),
        StructField("from", StringType()),
        StructField("created_time", TimestampType()),
        StructField("message", StringType()),
        StructField("permalink_url", StringType()),
        StructField("status_type", StringType()),
        StructField("is_published", BooleanType()),
        StructField("is_hidden", BooleanType()),
        StructField("is_expired", BooleanType()),
        StructField("shares", StringType()),
        StructField("full_picture", StringType()),
    ]
)

POST_INSIGHTS_SCHEMA = StructType(
    [
        *_AIRBYTE_COLUMNS,
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("period", StringType()),
        _insights_values(
            StructField("type", StringType()),
            StructField("object", StringType()),
            StructField("string", StringType()),
            StructField("integer", LongType()),
        ),
    ]
)

PAGE_INSIGHTS_SCHEMA = StructType(
    [
        *_AIRBYTE_COLUMNS,
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("period", StringType()),
        _insights_values(
            StructField("type", StringType()),
            StructField("object", StringType()),
            StructField("integer", LongType()),
        ),
    ]
)


@dataclass(frozen=True)
class FacebookStream:
    name: str
    schema: StructType
    dedupe_columns: tuple[str, ...] = ("id", "snapshot_date")

    @property
    def raw_path(self) -> str:
        """Glob sobre todas as páginas; o `page_id` sai do caminho de cada arquivo."""
        return f"{RAW_PREFIX}/*/{self.name}"

    @property
    def bronze_table_name(self) -> str:
        return f"{PLATFORM}/{self.name}"


@dataclass(frozen=True)
class SilverTableConfig:
    """Tabela silver montada a partir de um ou mais streams bronze.

    Ao contrário do TikTok, o silver aqui não é 1:1 com o bronze: `page` rende a dimensão e
    a série de seguidores, e `post_insights_snapshot` junta `post_insights` com o `shares` de
    `post`.
    """

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
        return f"{PLATFORM}/{self.name}"

    @property
    def silver_source_paths(self) -> list[str]:
        return [f"{PLATFORM}/{source}" for source in self.silver_sources]


FACEBOOK_STREAMS: tuple[FacebookStream, ...] = (
    FacebookStream(name="page", schema=PAGE_SCHEMA),
    FacebookStream(name="post", schema=POST_SCHEMA),
    FacebookStream(name="post_insights", schema=POST_INSIGHTS_SCHEMA),
    FacebookStream(name="page_insights", schema=PAGE_INSIGHTS_SCHEMA),
)

SILVER_TABLES: tuple[SilverTableConfig, ...] = (
    SilverTableConfig(name="pages", bronze_sources=("page",)),
    SilverTableConfig(name="page_followers_snapshot", bronze_sources=("page",)),
    SilverTableConfig(name="posts", bronze_sources=("post",)),
    SilverTableConfig(
        name="post_insights_snapshot", bronze_sources=("post_insights", "post")
    ),
    SilverTableConfig(name="page_insights_daily", bronze_sources=("page_insights",)),
)

GOLD_FACTS: tuple[GoldFactConfig, ...] = (
    GoldFactConfig(
        name="post_daily_metrics",
        silver_sources=("post_insights_snapshot", "posts", "pages"),
    ),
    GoldFactConfig(
        name="page_daily_metrics",
        silver_sources=("page_insights_daily", "page_followers_snapshot", "pages"),
    ),
)

# Métricas `lifetime` de post que viram coluna escalar no silver: nome na API → coluna.
# `post_clicks` fica de fora de propósito: falta em ~1/3 dos posts e, onde existe, é igual à
# soma de `post_clicks_by_type`.
POST_SCALAR_METRICS: dict[str, str] = {
    "post_media_view": "media_views_lifetime",
    "post_total_media_view_unique": "reach_lifetime",
}

# Métricas `lifetime` de post que chegam como JSON e viram MAP<STRING, BIGINT>.
POST_MAP_METRICS: dict[str, str] = {
    "post_reactions_by_type_total": "reactions_by_type_lifetime",
    "post_clicks_by_type": "clicks_by_type_lifetime",
}

# Tipos de reação que viram coluna no gold. `like` já inclui "care" (documentação da Meta).
REACTION_TYPES: tuple[str, ...] = ("like", "love", "haha", "wow", "sorry", "anger")

# Métricas de página, período `day`, que viram coluna no gold: nome na API → coluna.
PAGE_DAY_METRICS: dict[str, str] = {
    "page_media_view": "media_views",
    "page_total_media_view_unique": "viewers",
    "page_post_engagements": "post_engagements",
    "page_total_actions": "total_actions",
}

PAGE_FAN_ADDS_METRIC = "page_fan_adds_by_paid_non_paid_unique"
