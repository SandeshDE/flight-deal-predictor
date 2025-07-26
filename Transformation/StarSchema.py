# Databricks notebook source
service_credential = dbutils.secrets.get(scope="keyvaultScope",key="databricksSecret")

spark.conf.set("fs.azure.account.auth.type.fligjhtsg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.fligjhtsg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.fligjhtsg.dfs.core.windows.net", "f54bbf39-2dea-4397-89ad-6b31730973ce")
spark.conf.set("fs.azure.account.oauth2.client.secret.fligjhtsg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.fligjhtsg.dfs.core.windows.net", "https://login.microsoftonline.com/28ddd632-976b-4439-9198-12d3d583b2a5/oauth2/token")

# COMMAND ----------

origin=dbutils.widgets.get("origin")
destination=dbutils.widgets.get("destination")
path=f'abfss://silver@fligjhtsg.dfs.core.windows.net/EnrichedData/route:{origin}-{destination}'
dffinal=spark.read.format("delta").load(path)
dffinal.show(truncate=False)




# COMMAND ----------

# MAGIC %md
# MAGIC DateDimension

# COMMAND ----------

from pyspark.sql.functions import col, to_date, dayofweek, month, year
path = 'abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/Dimensions/Dim_Date'
date_df = (
    dffinal.select(dffinal.Departure_Date)
    
      .withColumn('Date', to_date(col('Departure_Date'), 'yyyy-MM-dd'))
      .withColumn('day_of_week', dayofweek(col('Date')))
      .withColumn('month', month(col('Date')))
      .withColumn('year', year(col('Date')))
)

date_df.show(truncate=False)


df1 = (
    date_df.dropDuplicates()
           .write.format('delta')
           .mode('append')
           .option("mergeSchema", "true")
           .save(path)
)


# COMMAND ----------

# MAGIC %md
# MAGIC AIrport-DIM

# COMMAND ----------

from pyspark.sql.functions import col, when

origin = dffinal.select('Origin').distinct().withColumnRenamed('Origin', 'IATACODE')
departure = dffinal.select('Destination').distinct().withColumnRenamed('Destination', 'IATACODE')

airport_df = origin.union(departure).distinct()

airport_df = airport_df.withColumn(
    'Airport_Name',
    when(col('IATACODE') == 'LHR', 'London Heathrow Airport - London')
    .when(col('IATACODE') == 'CDG', 'Charles de Gaulle Airport - Paris')
    .when(col('IATACODE') == 'AMS', 'Amsterdam Airport Schiphol - Amsterdam')
    .when(col('IATACODE') == 'BCN', 'Barcelona–El Prat Airport - Barcelona')
    .when(col('IATACODE') == 'FCO', 'Rome Fiumicino Airport - Rome')
    .otherwise('Unknown')
)
airport_df=airport_df.drop_duplicates()

display(airport_df)
airport_df.write.format('delta').mode('append').save(f'abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/Dimensions/Dim_Airport')


# COMMAND ----------

# MAGIC %md
# MAGIC Holiday-Dim

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
holiday_df = (
    dffinal.select(
        "holiday_name", 
        
        
        "origin_holiday_name", 
        "destination_holiday_name", 
        "Departure_Date"
    )
    .dropDuplicates()
    .filter(col("is_holiday_route") == 1)  
    .withColumn("holiday_key", monotonically_increasing_id())
)
holiday_df=holiday_df.withColumnRenamed('Departure_Date','Holiday_Date')
holiday_df=holiday_df.dropDuplicates()

holiday_df.write.format('delta').mode('append').option('mergeSchema', 'true').save(f'abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/Dimensions/Dim_Holiday')


# COMMAND ----------

# MAGIC %md
# MAGIC Joining the Dimensions with FactTables

# COMMAND ----------

# MAGIC %md
# MAGIC Fact+Dim_airport

# COMMAND ----------

from pyspark.sql.functions import col

origin_join = dffinal.join(
    airport_df.withColumnRenamed("IATACODE", "origin_code")
              .withColumnRenamed("airport_key", "origin_airport_key")
              .withColumnRenamed("Airport_Name", "Origin_Airport_Name"),
    dffinal["Origin"] == col("origin_code"),
    how="left"
)

# COMMAND ----------

dffinal=origin_join.join(
    airport_df.withColumnRenamed("IATACODE", "destination_code")
              .withColumnRenamed("airport_key", "destination_airport_key")
              .withColumnRenamed("Airport_Name", "Destination_Airport_Name"),
    origin_join["Destination"] == col("destination_code"),
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


final_fact_df=dffinal.dropDuplicates()

# COMMAND ----------

origin=dbutils.widgets.get("origin")
destination=dbutils.widgets.get("destination")
final_fact_df.write.mode('append').save(f'abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/Facts/Fact_Flight:{origin}-{destination}')

# COMMAND ----------

origin=dbutils.widgets.get("origin")
destination=dbutils.widgets.get("destination")
df = spark.read.format("delta").load(f'abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/Facts/Fact_Flight:{origin}-{destination}')
df.write.mode("append").option("header", "true").parquet(f'abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/FactsParquet/Fact_Flight:{origin}-{destination}')

# COMMAND ----------

"""
from pyspark.sql import SparkSession

# Spark session
spark = SparkSession.builder.getOrCreate()

# Load Parquet from ADLS
df = spark.read.parquet("abfss://gold@fligjhtsg.dfs.core.windows.net/StarSchema/FactsParquet/Fact_Flight")

# JDBC connection params
jdbcHostname = "sandesh"
jdbcPort = "1433"
jdbcDatabase = "FlightPrices"
jdbcUsername = "dbricks"
jdbcPassword = "Dsra@716,"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=false;trustServerCertificate=true;loginTimeout=30;"

connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write to SQL Server
df.write.mode("append").jdbc(url=jdbcUrl, table="fact_flight_data", properties=connectionProperties)
"""
