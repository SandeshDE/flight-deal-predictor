# Databricks notebook source
service_credential = dbutils.secrets.get(scope="keyvaultScope",key="databricksSecret")

spark.conf.set("fs.azure.account.auth.type.fligjhtsg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.fligjhtsg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.fligjhtsg.dfs.core.windows.net", "f54bbf39-2dea-4397-89ad-6b31730973ce")
spark.conf.set("fs.azure.account.oauth2.client.secret.fligjhtsg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.fligjhtsg.dfs.core.windows.net", "https://login.microsoftonline.com/28ddd632-976b-4439-9198-12d3d583b2a5/oauth2/token")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, udf
from pyspark.sql.types import BooleanType, StringType
import holidays

 

# Define your widgets and load data
origin = dbutils.widgets.get('origin')
destination = dbutils.widgets.get('destination')
path=f'abfss://silver@fligjhtsg.dfs.core.windows.net/VerifiedData/route:{origin}-{destination}'

df_fixed = spark.read.format('delta').load(path)

# Set up holiday calendars
uk_holidays = holidays.UnitedKingdom(years=[2023, 2024])
fr_holidays = holidays.France(years=[2023, 2024])
nl_holidays = holidays.Netherlands(years=[2023, 2024])
es_holidays = holidays.Spain(years=[2023, 2024])
it_holidays = holidays.Italy(years=[2023, 2024])

# Map IATA codes to holiday calendars
region_holidays = {
    "LHR": uk_holidays,
    "CDG": fr_holidays,
    "AMS": nl_holidays,
    "BCN": es_holidays,
    "FCO": it_holidays,
}

# UDF to check if a date is a holiday
def make_is_holiday_udf(region_holidays):
    def is_holiday(date, region):
        try:
            return date in region_holidays.get(region, {})
        except:
            return False
    return udf(is_holiday, BooleanType())

#  UDF to get holiday name
def make_holiday_name_udf(region_holidays):
    def get_holiday_name(date, region):
        try:
            holiday_obj = region_holidays.get(region)
            return holiday_obj.get(date) if holiday_obj and date in holiday_obj else None
        except:
            return None
    return udf(get_holiday_name, StringType())

# UDFs
is_holiday_udf = make_is_holiday_udf(region_holidays)
holiday_name_udf = make_holiday_name_udf(region_holidays)

# columns for origin/destination holiday flags and names
df_fixed = df_fixed.withColumn("is_origin_holiday", is_holiday_udf(col("departure_date"), col("origin"))) \
                   .withColumn("is_destination_holiday", is_holiday_udf(col("departure_date"), col("destination"))) \
                   .withColumn("origin_holiday_name", holiday_name_udf(col("departure_date"), col("origin"))) \
                   .withColumn("destination_holiday_name", holiday_name_udf(col("departure_date"), col("destination")))

#  is_holiday_route and single holiday_name column
df_fixed = df_fixed.withColumn("is_holiday_route", when(col("is_origin_holiday") | col("is_destination_holiday"), 1).otherwise(0)) \
                   .withColumn("holiday_name", coalesce(col("origin_holiday_name"), col("destination_holiday_name")))



#  Preview the final DataFrame
df_fixed.select("origin", "destination", "departure_date", "is_holiday_route", "holiday_name").where(col('holiday_name').isNotNull()).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Enriched Data:Silver/EnrichedData

# COMMAND ----------

path=f'abfss://silver@fligjhtsg.dfs.core.windows.net/EnrichedData/route:{origin}-{destination}'
df_fixed.write.format('delta').mode('append').save(path)

# COMMAND ----------

df=spark.read.format('delta').load(path)
df.show(3,truncate=False)