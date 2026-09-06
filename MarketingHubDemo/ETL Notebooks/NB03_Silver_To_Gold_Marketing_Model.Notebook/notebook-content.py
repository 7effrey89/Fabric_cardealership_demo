# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "c79785b5-3d77-4c73-86c7-185825278833",
# META       "known_warehouses": [
# META         {
# META           "id": "c79785b5-3d77-4c73-86c7-185825278833",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold: Schema-organized marketing star model
# Builds `dimensions.dim_*`, `facts.fct_*`, and `data_products.dp_*` Delta tables from the conformed Silver Lakehouse model, preserving the automotive product lineage established in Bronze and Silver.


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_WORKSPACE_ID = "69eb0045-4ad5-4d4f-b8e1-bd8076532719"
SILVER_LAKEHOUSE_ID = "8fe17275-0fa8-4427-bf33-943f5974f0c2"
SILVER_TABLES = f"abfss://{SILVER_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE_ID}/Tables"
silver = spark.read.format("delta").load(f"{SILVER_TABLES}/conformed/marketing_performance")

for schema_name in ["dimensions", "facts", "data_products"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

dim_date = (silver.select("event_date").distinct()
    .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
    .withColumnRenamed("event_date", "date")
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("date")))
    .withColumn("month_number", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week", F.date_format("date", "EEEE")))

dim_channel = (silver.select("channel", "source_system").distinct()
    .withColumn("channel_key", F.row_number().over(Window.orderBy("channel", "source_system"))))

dim_campaign = (silver.select("campaign_id", "campaign_name", "campaign_objective").distinct()
    .withColumn("campaign_key", F.row_number().over(Window.orderBy("campaign_id", "campaign_name", "campaign_objective"))))

dim_product = (silver.select("product_id", "product_name", "brand", "category").distinct()
    .withColumnRenamed("product_id", "product_key")
    .withColumn("list_price", F.lit(None).cast("decimal(18,2)"))
    .withColumn("rating", F.lit(None).cast("decimal(9,2)"))
    .withColumn("availability_status", F.lit("Not sourced")))

fact = (silver.alias("s")
    .join(dim_channel.alias("c"), ["channel", "source_system"], "inner")
    .join(dim_campaign.alias("ca"), ["campaign_id", "campaign_name", "campaign_objective"], "inner")
    .select(
        F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("c.channel_key"), F.col("ca.campaign_key"), F.col("product_id").alias("product_key"),
        "region", "device_category", "impressions", "clicks", "sessions", "spend", "conversions", "leads", "revenue", "video_views", "engagements",
        F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions")).alias("click_through_rate"),
        F.when(F.col("clicks") > 0, F.col("spend") / F.col("clicks")).alias("cost_per_click"),
        F.when(F.col("spend") > 0, F.col("revenue") / F.col("spend")).alias("return_on_ad_spend"),
        "batch_id", "ingestion_utc",
    ))

dp_campaign_daily = (fact.alias("f").join(dim_campaign.alias("c"), "campaign_key", "inner")
    .join(dim_channel.alias("ch"), "channel_key", "inner")
    .groupBy("date_key", "campaign_key", "campaign_name", "campaign_objective", "channel_key", "channel", "source_system")
    .agg(
        F.sum("impressions").alias("impressions"), F.sum("clicks").alias("clicks"), F.sum("sessions").alias("sessions"),
        F.sum("spend").alias("spend"), F.sum("conversions").alias("conversions"), F.sum("leads").alias("leads"), F.sum("revenue").alias("revenue"),
    ).withColumn("roas", F.when(F.col("spend") > 0, F.col("revenue") / F.col("spend"))))

tables = {
    "dimensions.dim_date": dim_date, "dimensions.dim_channel": dim_channel,
    "dimensions.dim_campaign": dim_campaign, "dimensions.dim_product": dim_product,
    "facts.fct_marketing_performance": fact, "data_products.dp_campaign_daily": dp_campaign_daily,
}
for table_name, frame in tables.items():
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

spark.sql("OPTIMIZE facts.fct_marketing_performance ZORDER BY (date_key, campaign_key)")
spark.sql("OPTIMIZE data_products.dp_campaign_daily ZORDER BY (date_key, campaign_key)")
print(", ".join(f"{name}={frame.count():,}" for name, frame in tables.items()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert fact.count() == 2160
assert fact.join(dim_date, "date_key", "left_anti").count() == 0
assert fact.join(dim_channel, "channel_key", "left_anti").count() == 0
assert dim_campaign.groupBy("campaign_key").count().filter(F.col("count") > 1).count() == 0
assert fact.join(dim_campaign, "campaign_key", "left_anti").count() == 0
assert fact.join(dim_product, "product_key", "left_anti").count() == 0
assert dim_product.filter(F.col("brand").isNull() | F.col("category").isNull()).count() == 0
assert dim_product.select("brand").distinct().count() == 6
assert dim_product.select("category").distinct().count() == 5
assert dp_campaign_daily.select("source_system").distinct().count() == 3

display(dp_campaign_daily.orderBy(F.desc("date_key"), "campaign_name", "source_system"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
