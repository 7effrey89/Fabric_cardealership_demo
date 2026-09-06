# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Bronze: Source-specific advertising landing tables
# Creates separate, narrow raw tables for Google Ads, Meta Ads, and LinkedIn Ads, including source-specific vehicle model, automotive brand, and vehicle category fields. Google Analytics and the unused product catalog are intentionally excluded.


# CELL ********************

from datetime import date, datetime, timedelta, timezone
import random
import uuid

random.seed(20260904)
batch_id = str(uuid.uuid4())
ingestion_utc = datetime.now(timezone.utc).replace(tzinfo=None)
start_date = date.today() - timedelta(days=179)
campaigns = [
    (101, "Always-on Brand", "Awareness"),
    (102, "Marketing Hub Launch", "Demand Generation"),
    (103, "Analytics Webinar", "Lead Generation"),
    (104, "Customer Stories", "Consideration"),
]
regions = ["North America", "Europe", "Asia Pacific"]
devices = ["Desktop", "Mobile", "Tablet"]
brands = ["Apex Motors", "Aurora Automotive", "Contoso Motors", "Fabrikam Auto", "Northstar Vehicles", "Tailspin Motors"]
categories = ["Electric SUV", "Sedan", "Crossover", "Sports Car", "Pickup Truck"]
model_names = [
    ["Summit EV", "Verona", "Trailcrest", "Velocity GT", "Atlas XT"],
    ["Borealis E-SUV", "Solstice", "Horizon Cross", "Nova RS", "Tundra Max"],
    ["TerraVolt", "Regent", "Metro Trek", "Falcon XR", "Frontier Pro"],
    ["Eclipse X", "Monarch", "Venture", "Tempest R", "Workhorse HD"],
    ["Polar EV", "Meridian", "Pathfinder", "Comet S", "Ridgeline TX"],
    ["Cyclone E-X", "Aero", "Drift Cross", "Raptor Z", "Storm Hauler"],
]

def vehicle_attributes(product_id):
    brand_index = (product_id - 1) // 5
    category_index = (product_id - 1) % 5
    return model_names[brand_index][category_index], brands[brand_index], categories[category_index]

gads_rows, meta_rows, li_rows = [], [], []
for day_offset in range(180):
    event_date = start_date + timedelta(days=day_offset)
    seasonality = 1.0 + 0.18 * ((day_offset % 30) / 29.0)
    for campaign_id, campaign_name, objective in campaigns:
        product_id = 1 + ((campaign_id + day_offset) % 30)
        vehicle_model, vehicle_brand, vehicle_category = vehicle_attributes(product_id)
        region = regions[(campaign_id + day_offset) % len(regions)]
        device = devices[(campaign_id + day_offset) % len(devices)]

        gads_impressions = int(random.randint(8000, 43000) * seasonality)
        gads_clicks = max(1, int(gads_impressions * random.uniform(0.018, 0.055)))
        gads_spend = round(gads_clicks * 1.35 * random.uniform(0.85, 1.18), 2)
        gads_conversions = max(0, int(gads_clicks * random.uniform(0.016, 0.065)))
        gads_revenue = round(gads_conversions * random.uniform(110, 500), 2)
        gads_rows.append((event_date, campaign_id, campaign_name, objective, product_id, vehicle_model, vehicle_brand, vehicle_category, region, device, gads_impressions, gads_clicks, gads_spend, gads_conversions, gads_revenue, ingestion_utc, batch_id))

        meta_impressions = int(random.randint(11000, 50000) * seasonality)
        meta_clicks = max(1, int(meta_impressions * random.uniform(0.012, 0.042)))
        meta_spend = round(meta_clicks * 0.82 * random.uniform(0.84, 1.16), 2)
        meta_leads = max(0, int(meta_clicks * random.uniform(0.022, 0.09)))
        meta_video_views = int(meta_impressions * random.uniform(0.08, 0.38))
        meta_engagements = int(meta_impressions * random.uniform(0.009, 0.045))
        meta_revenue = round(meta_leads * random.uniform(65, 270), 2)
        meta_rows.append((event_date, campaign_id, campaign_name, product_id, vehicle_model, vehicle_brand, vehicle_category, region, meta_impressions, meta_clicks, meta_spend, meta_leads, meta_video_views, meta_engagements, meta_revenue, ingestion_utc, batch_id))

        li_impressions = int(random.randint(5000, 26000) * seasonality)
        li_clicks = max(1, int(li_impressions * random.uniform(0.009, 0.032)))
        li_spend = round(li_clicks * 2.10 * random.uniform(0.88, 1.20), 2)
        li_leads = max(0, int(li_clicks * random.uniform(0.03, 0.12)))
        li_conversions = max(0, int(li_leads * random.uniform(0.18, 0.48)))
        li_revenue = round(li_conversions * random.uniform(180, 720), 2)
        li_rows.append((event_date, campaign_id, campaign_name, objective, product_id, vehicle_model, vehicle_brand, vehicle_category, region, li_impressions, li_clicks, li_spend, li_leads, li_conversions, li_revenue, ingestion_utc, batch_id))

source_frames = {
    "src_google_ads_raw": spark.createDataFrame(gads_rows, ["gads_date", "gads_campaign_id", "gads_campaign_name", "gads_objective", "gads_product_id", "gads_vehicle_model", "gads_vehicle_brand", "gads_vehicle_category", "gads_region", "gads_device", "gads_impressions", "gads_clicks", "gads_spend", "gads_conversions", "gads_revenue", "gads_ingested_utc", "gads_batch_id"]),
    "src_meta_ads_raw": spark.createDataFrame(meta_rows, ["meta_date", "meta_campaign_id", "meta_campaign_name", "meta_product_id", "meta_auto_model", "meta_auto_make", "meta_vehicle_class", "meta_region", "meta_impressions", "meta_clicks", "meta_spend", "meta_leads", "meta_video_views", "meta_engagements", "meta_revenue", "meta_ingested_utc", "meta_batch_id"]),
    "src_linkedin_ads_raw": spark.createDataFrame(li_rows, ["li_date", "li_campaign_id", "li_campaign_name", "li_objective", "li_product_id", "li_promoted_model", "li_manufacturer", "li_segment", "li_region", "li_impressions", "li_clicks", "li_spend", "li_leads", "li_conversions", "li_revenue", "li_ingested_utc", "li_batch_id"]),
}
for table_name, frame in source_frames.items():
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

for unused_table in ["src_google_analytics_raw", "src_product_catalog_raw", "digital_marketing_signals_raw", "api_products_raw"]:
    spark.sql(f"DROP TABLE IF EXISTS {unused_table}")

print(f"Bronze batch {batch_id}: " + ", ".join(f"{name}={frame.count():,}" for name, frame in source_frames.items()))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected_tables = {
    "src_google_ads_raw": (720, "gads_"),
    "src_meta_ads_raw": (720, "meta_"),
    "src_linkedin_ads_raw": (720, "li_"),
}
for table_name, (expected_count, prefix) in expected_tables.items():
    frame = spark.table(table_name)
    assert frame.count() == expected_count, f"Unexpected row count for {table_name}"
    assert all(column.startswith(prefix) for column in frame.columns), f"Unexpected column prefix in {table_name}"
    assert len(frame.columns) <= 17, f"{table_name} is unnecessarily wide"
    assert frame.select([column for column in frame.columns if column.endswith(("model", "make", "brand", "manufacturer", "category", "class", "segment"))]).dropna().count() == expected_count

for removed_table in ["src_google_analytics_raw", "src_product_catalog_raw", "digital_marketing_signals_raw", "api_products_raw"]:
    assert not spark.catalog.tableExists(removed_table), f"Unused table still exists: {removed_table}"

display(spark.createDataFrame([
    (name, spark.table(name).count(), len(spark.table(name).columns), prefix)
    for name, (_, prefix) in expected_tables.items()
], ["bronze_table", "row_count", "column_count", "source_prefix"]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
