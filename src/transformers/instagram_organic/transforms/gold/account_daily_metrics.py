from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.transformers.instagram_organic.tables import GoldFactConfig


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """Métricas diárias da conta: conta × `metric_date`. Função pura, sem I/O.

    `reach` é o **total da conta, inclui anúncios** — nunca some com o alcance dos posts (é
    único, e os posts são orgânicos). `reach_week` e `reach_days_28` são janelas móveis e não
    somam. `is_partial` marca o dia ainda em andamento na última leitura.

    `followers_count`, `follows_count` e `media_count` vêm da foto do mesmo dia
    (`snapshot_date = metric_date`: a foto que fecha o dia D traz os números ao fim de D).
    """
    daily = sources["account_insights_daily"]
    followers = sources["account_followers_snapshot"].select(
        col("business_account_id"),
        col("snapshot_date").alias("metric_date"),
        col("followers_count"),
        col("follows_count"),
        col("media_count"),
    )
    accounts = sources["accounts"].select(col("business_account_id"), col("username"))

    return (
        daily.join(followers, ["business_account_id", "metric_date"], "left")
        .join(accounts, "business_account_id", "left")
        .select(
            col("business_account_id"),
            col("username"),
            col("metric_date"),
            col("is_partial"),
            col("reach"),
            col("reach_week"),
            col("reach_days_28"),
            col("new_followers"),
            col("followers_count"),
            col("follows_count"),
            col("media_count"),
        )
    )
