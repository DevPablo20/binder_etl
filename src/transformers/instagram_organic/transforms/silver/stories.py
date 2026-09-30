from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round as round_, unix_timestamp

from src.transformers.snapshots import local_date


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Um story por linha, com as métricas da última leitura.

    Métrica de story só existe enquanto o story está no ar (24h). Com o sync de hora em hora, a
    última leitura acontece até 1h antes de o story expirar e vale como o número final;
    `hours_live_at_last_read` diz com que idade o story foi lido pela última vez. O bronze já
    deixou uma linha por story.

    Story sem linha de insights sobrevive (`LEFT JOIN`): a Meta devolve erro para métrica com
    valor menor que 5.
    """
    stories = sources["stories"]
    insights = sources["story_insights"].select(
        col("id"),
        col("reach"),
        col("views"),
        col("shares"),
        col("follows"),
        col("replies"),
        col("profile_visits"),
        col("total_interactions"),
        col("_airbyte_extracted_at").alias("last_read_at"),
    )

    return stories.join(insights, "id", "left").select(
        col("id").cast("string").alias("story_id"),
        col("business_account_id").cast("string").alias("business_account_id"),
        col("timestamp").alias("created_at"),
        local_date(col("timestamp")).alias("created_date"),
        col("media_type"),
        col("caption"),
        col("permalink"),
        col("shortcode"),
        col("reach"),
        col("views"),
        col("shares"),
        col("follows"),
        col("replies"),
        col("profile_visits"),
        col("total_interactions"),
        col("last_read_at"),
        round_(
            (unix_timestamp(col("last_read_at")) - unix_timestamp(col("timestamp"))) / 3600, 2
        ).alias("hours_live_at_last_read"),
        col("_airbyte_raw_id"),
        col("_airbyte_extracted_at"),
        col("_airbyte_meta"),
    )
