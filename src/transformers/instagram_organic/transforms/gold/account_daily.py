from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.transformers.instagram_organic.tables import GOLD_PLATFORM, GoldFactConfig
from src.transformers.organic_gold import TOTAL, conform


def transform(sources: dict[str, DataFrame], _fact: GoldFactConfig) -> DataFrame:
    """`account_daily` do Instagram: a conta num dia. Função pura.

    `reach` é o **total da conta, inclui anúncios**, e é único — nunca é a soma do alcance dos
    posts. `is_partial` marca o dia ainda em andamento na última leitura: o sync seguinte o
    fecha. `followers_total` vem da foto do mesmo dia.
    """
    daily = sources["account_insights_daily"]
    followers = sources["account_followers_snapshot"].select(
        col("business_account_id"),
        col("snapshot_date").alias("metric_date"),
        col("followers_count").alias("followers_total"),
    )
    accounts = sources["accounts"].select(col("business_account_id"), col("username"))

    df = (
        daily.join(followers, ["business_account_id", "metric_date"], "left")
        .join(accounts, "business_account_id", "left")
        .select(
            col("metric_date").alias("date"),
            col("business_account_id").alias("account_id"),
            col("username").alias("account_name"),
            col("is_partial"),
            col("reach"),
            col("new_followers"),
            col("followers_total"),
        )
    )
    return conform(df, "account_daily", GOLD_PLATFORM, metrics_scope=lit(TOTAL))
