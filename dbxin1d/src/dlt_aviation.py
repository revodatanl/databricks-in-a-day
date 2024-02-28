# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# COMMAND ----------

@dlt.table(
  name="bronze_aviation",
  comment="BRONZE layer table for flight between Eindhoven and Budapest"
  )
def raw_to_bronze():
  return (
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/Volumes/databricks_training/raw/aviation/")
    .withColumn("source", F.col("_metadata.file_path"))
    .withColumn("ingestion_ts", F.current_timestamp())
  )

# COMMAND ----------

@dlt.table(
  name="silver_airports",
  comment="Mapping table for airport codes and their meaning",
  )
def silver_airports():
  return (
    spark.createDataFrame([{'airport_code': 'EIN', 'city': "EINDHOVEN"}, 
                           {'airport_code': 'BUD', 'city': "BUDAPEST"}])
  )

# COMMAND ----------

flight_schema = """
ARRAY<STRUCT<arrivalStation: STRING, departureDate: STRING, departureDates: ARRAY<STRING>, departureStation: STRING, hasMacFlight: BOOLEAN, originalPrice: STRUCT<amount: DOUBLE, currencyCode: STRING>, price: STRUCT<amount: DOUBLE, currencyCode: STRING>, priceType: STRING>>
"""

def create_silver_table(direction:str):
  """
  Helper function not to repeat code when doing the union
  """
  return(
    dlt
      .read_stream("bronze_aviation")
      .withColumn(direction, F.explode(F.from_json(direction, flight_schema)))
      .selectExpr("to_date(substring(split(source, '/')[5], 0, 10)) as date", f"{direction}.*")
  )

@dlt.table(
  name="silver_flights",
  comment="SILVER layer table for flight from Eindhoven and Budapest",
  )
def bronze_to_silver():
  return (
    create_silver_table("outboundFlights").unionByName(create_silver_table("returnFlights"))
  )

# COMMAND ----------

# Standalone function so that I can easily negate it and create a quarantine table
def get_gold_table():
    join_criteria_arr = F.col("arrivalStation") == F.col("airport_code")
    join_criteria_dep = F.col("departureStation") == F.col("airport_code")

    return (
        dlt.read_stream("silver_flights")
        .join(dlt.read("silver_airports"), on=join_criteria_arr)
        .withColumnRenamed("city", "arrival_station_name")
        .drop("airport_code")  # this is to get rid of ambiguity for the next join
        .join(dlt.read("silver_airports"), on=join_criteria_dep)
        .withColumnRenamed("city", "departure_station_name")
        .selectExpr(
            "departure_station_name",
            "arrival_station_name",
            "date as price_publish_date",
            "price.amount as price",
            "to_date(departureDate) as departure_date"
        )
    )

# COMMAND ----------


@dlt.table(
    name="gold_aviation",
    comment="GOLD level table for dashboarding",
)
@dlt.expect_or_drop("valid_price", "price > 0")
def gold_aviation():
    return get_gold_table()
    

# COMMAND ----------


@dlt.table(
    name="gold_aviation_quarantine",
    comment="This is the table for invalid records that fail the validation",
)
@dlt.expect_or_drop("invalid_price", "price <= 0")
def gold_aviation():
    return get_gold_table()
    

# COMMAND ----------

