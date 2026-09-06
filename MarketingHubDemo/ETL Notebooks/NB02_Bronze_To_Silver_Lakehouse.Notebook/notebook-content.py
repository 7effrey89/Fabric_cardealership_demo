# Fabric notebook source


# MARKDOWN ********************

# # Silver: Conformed advertising model in a Lakehouse
# Normalizes Google Ads, Meta Ads, and LinkedIn Ads into one canonical performance table, including governed vehicle model, automotive brand, and vehicle category attributes. Unused Google Analytics, product-catalog, and enriched duplicate tables are removed.


# CELL ********************

from pyspark.sql import functions as F

BRONZE_WORKSPACE_ID = "69eb0045-4ad5-4d4f-b8e1-bd8076532719"
BRONZE_LAKEHOUSE_ID = "535f34b5-1f64-47d5-9046-aaf246e3ae82"
BRONZE_TABLES = f"abfss://{BRONZE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{BRONZE_LAKEHOUSE_ID}/Tables"

def read_bronze(table_name):
    return spark.read.format("delta").load(f"{BRONZE_TABLES}/{table_name}")

gads = read_bronze("src_google_ads_raw")
meta = read_bronze("src_meta_ads_raw")
linkedin = read_bronze("src_linkedin_ads_raw")
null_long = F.lit(None).cast("long")
null_text = F.lit(None).cast("string")

gads_common = gads.select(
    F.col("gads_date").alias("event_date"), F.lit("Google Ads").alias("source_system"), F.lit("Paid Search").alias("channel"),
    F.col("gads_campaign_id").alias("campaign_id"), F.col("gads_campaign_name").alias("campaign_name"), F.col("gads_objective").alias("campaign_objective"),
    F.col("gads_product_id").alias("product_id"), F.col("gads_vehicle_model").alias("product_name"), F.col("gads_vehicle_brand").alias("brand"), F.col("gads_vehicle_category").alias("category"),
    F.col("gads_region").alias("region"), F.col("gads_device").alias("device_category"),
    F.col("gads_impressions").cast("long").alias("impressions"), F.col("gads_clicks").cast("long").alias("clicks"), F.col("gads_clicks").cast("long").alias("sessions"),
    F.col("gads_spend").cast("double").alias("spend"), F.col("gads_conversions").cast("long").alias("conversions"), F.col("gads_conversions").cast("long").alias("leads"),
    F.col("gads_revenue").cast("double").alias("revenue"), null_long.alias("video_views"), null_long.alias("engagements"),
    F.col("gads_ingested_utc").alias("ingestion_utc"), F.col("gads_batch_id").alias("batch_id"),
)

meta_common = meta.select(
    F.col("meta_date").alias("event_date"), F.lit("Meta Ads").alias("source_system"), F.lit("Paid Social").alias("channel"),
    F.col("meta_campaign_id").alias("campaign_id"), F.col("meta_campaign_name").alias("campaign_name"), F.lit("Social Engagement").alias("campaign_objective"),
    F.col("meta_product_id").alias("product_id"), F.col("meta_auto_model").alias("product_name"), F.col("meta_auto_make").alias("brand"), F.col("meta_vehicle_class").alias("category"),
    F.col("meta_region").alias("region"), null_text.alias("device_category"),
    F.col("meta_impressions").cast("long").alias("impressions"), F.col("meta_clicks").cast("long").alias("clicks"), F.col("meta_clicks").cast("long").alias("sessions"),
    F.col("meta_spend").cast("double").alias("spend"), F.col("meta_leads").cast("long").alias("conversions"), F.col("meta_leads").cast("long").alias("leads"),
    F.col("meta_revenue").cast("double").alias("revenue"), F.col("meta_video_views").cast("long").alias("video_views"), F.col("meta_engagements").cast("long").alias("engagements"),
    F.col("meta_ingested_utc").alias("ingestion_utc"), F.col("meta_batch_id").alias("batch_id"),
)

linkedin_common = linkedin.select(
    F.col("li_date").alias("event_date"), F.lit("LinkedIn Ads").alias("source_system"), F.lit("Paid Social B2B").alias("channel"),
    F.col("li_campaign_id").alias("campaign_id"), F.col("li_campaign_name").alias("campaign_name"), F.col("li_objective").alias("campaign_objective"),
    F.col("li_product_id").alias("product_id"), F.col("li_promoted_model").alias("product_name"), F.col("li_manufacturer").alias("brand"), F.col("li_segment").alias("category"),
    F.col("li_region").alias("region"), null_text.alias("device_category"),
    F.col("li_impressions").cast("long").alias("impressions"), F.col("li_clicks").cast("long").alias("clicks"), F.col("li_clicks").cast("long").alias("sessions"),
    F.col("li_spend").cast("double").alias("spend"), F.col("li_conversions").cast("long").alias("conversions"), F.col("li_leads").cast("long").alias("leads"),
    F.col("li_revenue").cast("double").alias("revenue"), null_long.alias("video_views"), null_long.alias("engagements"),
    F.col("li_ingested_utc").alias("ingestion_utc"), F.col("li_batch_id").alias("batch_id"),
)

performance = (gads_common.unionByName(meta_common).unionByName(linkedin_common)
    .dropDuplicates(["event_date", "source_system", "campaign_id", "product_id", "region"])
    .filter(F.col("event_date").isNotNull() & F.col("campaign_id").isNotNull())
    .filter((F.coalesce(F.col("spend"), F.lit(0.0)) >= 0) & (F.coalesce(F.col("revenue"), F.lit(0.0)) >= 0))
    .filter(F.col("clicks").isNull() | F.col("impressions").isNull() | (F.col("clicks") <= F.col("impressions")))
    .withColumn("spend", F.bround("spend", 2).cast("decimal(18,2)"))
    .withColumn("revenue", F.bround("revenue", 2).cast("decimal(18,2)"))
    .withColumn("quality_status", F.lit("Valid"))
)

for schema_name in ["conformed", "audit"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("conformed.marketing_performance")
source_counts = performance.groupBy("source_system").count().withColumn("validated_utc", F.current_timestamp())
source_counts.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("audit.source_row_counts")

for unused_table in ["conformed.marketing_performance_enriched", "reference.product_catalog"]:
    spark.sql(f"DROP TABLE IF EXISTS {unused_table}")

print(f"Silver conformed rows: {performance.count():,}")


# CELL ********************

silver = spark.table("conformed.marketing_performance")
assert silver.count() == 2160, "Expected three ad sources across 180 days and four campaigns"
assert silver.select("source_system").distinct().count() == 3
assert silver.filter(F.col("product_name").isNull() | F.col("brand").isNull() | F.col("category").isNull()).count() == 0
assert silver.select("brand").distinct().count() == 6
assert silver.select("category").distinct().count() == 5
assert silver.filter(F.col("quality_status") != "Valid").count() == 0
assert not spark.catalog.tableExists("conformed.marketing_performance_enriched")
assert not spark.catalog.tableExists("reference.product_catalog")

display(silver.groupBy("source_system", "channel").agg(
    F.count("*").alias("rows"), F.sum("spend").alias("spend"),
    F.sum("revenue").alias("revenue"), F.sum("conversions").alias("conversions"),
).orderBy("source_system"))

