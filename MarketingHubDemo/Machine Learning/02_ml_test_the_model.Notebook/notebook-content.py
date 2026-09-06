# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8fe17275-0fa8-4427-bf33-943f5974f0c2",
# META       "default_lakehouse_name": "MarketingHub_Silver",
# META       "default_lakehouse_workspace_id": "69eb0045-4ad5-4d4f-b8e1-bd8076532719",
# META       "known_lakehouses": [
# META         {
# META           "id": "8fe17275-0fa8-4427-bf33-943f5974f0c2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Using the ML model on existing data
# 
# this sample uses an existing table against the Model, and also saves the output

# CELL ********************

# Ensure the correct scikit-learn version required by the MLflow model
# The error shows the model expects scikit-learn==1.5.1 but the environment has 1.2.2
%pip install scikit-learn==1.5.1

import mlflow
from synapse.ml.predict import MLFlowTransformer

# Read source data
df = spark.read.format("delta").load(
    "abfss://69eb0045-4ad5-4d4f-b8e1-bd8076532719@onelake.dfs.fabric.microsoft.com/8fe17275-0fa8-4427-bf33-943f5974f0c2/Tables/dbo/marketing_performance"
)

# Load and apply the MLflow model
# Key change is above: aligning scikit-learn version so cloudpickle can deserialize
model = MLFlowTransformer(
    inputCols=[
        "event_date","ingestion_utc","campaign_id","product_id","impressions",
        "clicks","sessions","video_views","engagements","conversions","leads",
        "revenue","source_system","channel","campaign_name","campaign_objective",
        "product_name","brand","category","region","device_category","batch_id",
        "quality_status"
    ],
    outputCol="predicted_spend",
    modelName="S_AutoML-AutoMLModel",
    modelVersion=1
)

# Transform the dataframe with predictions
df_pred = model.transform(df)

# Write results back to Delta
(df_pred
    .write
    .format("delta")
    .mode("overwrite")
    .save("abfss://69eb0045-4ad5-4d4f-b8e1-bd8076532719@onelake.dfs.fabric.microsoft.com/8fe17275-0fa8-4427-bf33-943f5974f0c2/Tables/ml_demo/batch_score")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Rebuild manual_df with correct dtypes (naive datetimes & numeric IDs) and get prediction
%pip install scikit-learn==1.5.1

import mlflow
from synapse.ml.predict import MLFlowTransformer

from datetime import date, datetime
from pyspark.sql.types import (
    StructType, StructField,
    DateType, TimestampType, DoubleType, StringType
)

# Schema must match the MLflow model signature exactly
schema = StructType([
    StructField("event_date", DateType(), False),              # datetime (date)
    StructField("ingestion_utc", TimestampType(), False),      # datetime (timestamp, tz‑naive)
    StructField("campaign_id", DoubleType(), False),
    StructField("product_id", DoubleType(), False),
    StructField("impressions", DoubleType(), False),
    StructField("clicks", DoubleType(), False),
    StructField("sessions", DoubleType(), False),
    StructField("video_views", DoubleType(), False),
    StructField("engagements", DoubleType(), False),
    StructField("conversions", DoubleType(), False),
    StructField("leads", DoubleType(), False),
    StructField("revenue", DoubleType(), False),
    StructField("source_system", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("campaign_objective", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("category", StringType(), False),
    StructField("region", StringType(), False),
    StructField("device_category", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("quality_status", StringType(), False),
])

# Edit this tuple with your own test values.
# NOTE:
# - event_date: use datetime.date (no timezone)
# - ingestion_utc: use datetime.datetime WITHOUT tzinfo (naive)
# - campaign_id/product_id and metrics: numeric values (float/int)

data = [(
    date(2024, 9, 1),                 # event_date
    datetime(2024, 9, 1, 12, 0, 0),   # ingestion_utc (naive, interpreted as UTC by Spark)
    123.0,                             # campaign_id
    1.0,                               # product_id
    100000.0,                          # impressions
    5000.0,                            # clicks
    4000.0,                            # sessions
    2000.0,                            # video_views
    3000.0,                            # engagements
    400.0,                             # conversions
    50.0,                              # leads
    100000.0,                          # revenue
    "Google Ads",                     # source_system
    "Search",                         # channel
    "Back to School",                 # campaign_name
    "Awareness",                      # campaign_objective
    "Laptop",                         # product_name
    "BrandX",                         # brand
    "Electronics",                    # category
    "US",                             # region
    "Mobile",                         # device_category
    "batch_1",                        # batch_id
    "Approved"                        # quality_status
)]

manual_df = spark.createDataFrame(data, schema=schema)

# Columns for the transformer must match the schema order/names
input_cols = [
    "event_date","ingestion_utc","campaign_id","product_id","impressions",
    "clicks","sessions","video_views","engagements","conversions","leads",
    "revenue","source_system","channel","campaign_name","campaign_objective",
    "product_name","brand","category","region","device_category","batch_id",
    "quality_status"
]

model = MLFlowTransformer(
    inputCols=input_cols,
    outputCol="predicted_spend",
    modelName="S_AutoML-AutoMLModel",
    modelVersion=1
)

manual_pred = model.transform(manual_df)

# display(manual_pred)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Show the prediction

# CELL ********************

display(manual_pred)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
