# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a501d02a-648b-4c7d-8ef9-d4d736ca5241",
# META       "default_lakehouse_name": "MarketingHub_Gold",
# META       "default_lakehouse_workspace_id": "69eb0045-4ad5-4d4f-b8e1-bd8076532719",
# META       "known_lakehouses": [
# META         {
# META           "id": "a501d02a-648b-4c7d-8ef9-d4d736ca5241"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM MarketingHub_Gold.facts.fct_marketing_performance ORDER BY date_key DESC LIMIT 100")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

from pyspark.sql import functions as F
import uuid
import random

# ---------------------------
# 1. Parameters you control
# ---------------------------
# NOTE: business variables you want to control

date_key = 20260909              # e.g. yyyymmdd
return_on_ad_spend = 2.0         # your chosen ROAS
spend = 2011                     # your chosen spend
batch_id = "Manual_insert_jef"   # your chosen batch ID (string)

# ---------------------------
# 2. Pick valid *_key values from dimensions
# ---------------------------

date_key_val = date_key  # assumes this exists in dim_date

campaign_key_val = (
    spark.table("MarketingHub_Gold.dimensions.dim_campaign")
         .select("campaign_key")
         .orderBy(F.rand())
         .limit(1)
         .collect()[0]["campaign_key"]
)

channel_key_val = (
    spark.table("MarketingHub_Gold.dimensions.dim_channel")
         .select("channel_key")
         .orderBy(F.rand())
         .limit(1)
         .collect()[0]["channel_key"]
)

product_key_val = (
    spark.table("MarketingHub_Gold.dimensions.dim_product")
         .select("product_key")
         .orderBy(F.rand())
         .limit(1)
         .collect()[0]["product_key"]
)

# ---------------------------
# 3. Build 1 synthetic row with your controls + randoms
# ---------------------------

synthetic_row = {
    # Foreign keys (must be valid)
    "date_key": date_key_val,
    "campaign_key": campaign_key_val,
    "channel_key": channel_key_val,
    "product_key": product_key_val,

    # Controlled metrics & identifiers
    "batch_id": batch_id,
    "spend": float(spend),
    "return_on_ad_spend": float(return_on_ad_spend),

    # Other metrics – random examples; adjust if you like
    "impressions": random.randint(1_000, 50_000),
    "clicks": random.randint(50, 2_000),
    "conversions": random.randint(1, 200),

    # Derived metric
    "revenue": float(spend * return_on_ad_spend),

    # Tracking
    "synthetic_batch_row_id": str(uuid.uuid4()),
}

# Create a single-row DataFrame based on the simple dict
base_df_new = spark.createDataFrame([synthetic_row])

# ---------------------------
# 4. Align EXACTLY to the existing fact table schema
#    - add missing columns with null/defaults & correct types
#    - cast existing columns to table types
#    - reorder columns to match the table
# ---------------------------

fact_df = spark.table("MarketingHub_Gold.facts.fct_marketing_performance")
fact_schema = fact_df.schema

df_new = base_df_new

# 4a. Ensure ALL columns from the fact table exist in df_new
for field in fact_schema:
    if field.name not in df_new.columns:
        # add as null with correct data type
        df_new = df_new.withColumn(field.name, F.lit(None).cast(field.dataType.simpleString()))

# 4b. Cast overlapping columns to the exact fact-table data types
for field in fact_schema:
    if field.name in df_new.columns:
        df_new = df_new.withColumn(field.name, F.col(field.name).cast(field.dataType.simpleString()))

# 4c. Reorder columns to match the fact table schema exactly
ordered_cols = [field.name for field in fact_schema]
df_new_aligned = df_new.select(ordered_cols)

# ---------------------------
# 5. Append to the fact table
# ---------------------------
(
    df_new_aligned
    .write
    .mode("append")
    .saveAsTable("MarketingHub_Gold.facts.fct_marketing_performance")
)

print("Inserted 1 aligned synthetic row into MarketingHub_Gold.facts.fct_marketing_performance")
display(df_new_aligned)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
