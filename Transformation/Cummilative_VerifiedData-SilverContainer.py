# Databricks notebook source
display(dbutils.secrets.listScopes())
dbutils.secrets.list("keyvaultScope")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="keyvaultScope",key="databricksSecret")

spark.conf.set("fs.azure.account.auth.type.fligjhtsg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.fligjhtsg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.fligjhtsg.dfs.core.windows.net", "f54bbf39-2dea-4397-89ad-6b31730973ce")
spark.conf.set("fs.azure.account.oauth2.client.secret.fligjhtsg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.fligjhtsg.dfs.core.windows.net", "https://login.microsoftonline.com/28ddd632-976b-4439-9198-12d3d583b2a5/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@fligjhtsg.dfs.core.windows.net/")

# COMMAND ----------

origin=dbutils.widgets.get("origin")
destination=dbutils.widgets.get("destination")
year=[2019,2022,2023,2024]
route=f'{origin}-{destination}'
dataframes=[]
for i in year:
    print(f'Year:{year}')

   
    source=f'Amadeous-{i}'
    x=12
    for j in range(1,x+1):
        #zfill(2):it pads the numericalstring with 0,to make it 2 digits
        j=str(j).zfill(2)
        file_path=f"abfss://bronze@fligjhtsg.dfs.core.windows.net/raw/{source}/route:{route}/Month={j}/"
        print(file_path)
        try :
            df=spark.read.format("json").option('header','true').load(file_path)
            dataframes.append(df)
        except Exception as e:
            print(e)
if dataframes:
    df_final=dataframes[0]
    for df in dataframes[1:]:
        df_final=df_final.union(df)
df_final.count()


        
        





# COMMAND ----------

df_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Storing the Cummilative Data into the silver container as a cummilativedata/route:This contains the data range(2019-2024,exc:2020,2021)

# COMMAND ----------

origin=dbutils.widgets.get('origin')
destination=dbutils.widgets.get('destination')

savepath=f'abfss://silver@fligjhtsg.dfs.core.windows.net/Cummilative_routes:/{origin}-{destination}/'
df_final.write.format('delta').mode('overwrite').save(savepath)

# COMMAND ----------

paths=f'abfss://silver@fligjhtsg.dfs.core.windows.net/Cummilative_routes:/{origin}-{destination}/'
df_final=spark.read.format('delta').load(paths)
from pyspark.sql.functions import col, lit, explode,expr
#flattening the data column : to distribute the prices across the columns
df_exploded = df_final.select(explode(col('data')).alias('data'))
#flattening the json(array) into a structured dataframe
df_flattened = df_exploded.select(
    col('data.currencyCode').alias('CurrencyCode'),
    col('data.departureDate').alias('Departure_Date'),
    col('data.destination.iataCode').alias('Destination'),
    col('data.oneWay').alias('OneWay'),
    col('data.origin.iataCode').alias('Origin'),
    expr('data.priceMetrics.amount[0]').alias('Economy'),
    expr('data.priceMetrics.amount[1]').alias('PremiumEconomy'),
    expr('data.priceMetrics.amount[2]').alias('Business'),
    expr('data.priceMetrics.amount[3]').alias('First')
)
df_flattened.printSchema()


# COMMAND ----------

from pyspark.sql.functions import expr

df_modified = df_flattened.withColumn("Economy", expr("ROUND(rand() * (100 - 40) + 40, 2)")) \
    .withColumn("PremiumEconomy", expr("ROUND(rand() * (180 - 150) + 150, 2)")) \
    .withColumn("Business", expr("ROUND(rand() * (350 - 200) + 200, 2)")) \
    .withColumn("First", expr("ROUND(rand() * (500 - 380) + 380, 2)"))
df_modified.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Check for the Data Redundancy and stores the VerifiedData in SIlver container

# COMMAND ----------


savepath=f'abfss://silver@fligjhtsg.dfs.core.windows.net/VerifiedData/route:{origin}-{destination}'

def chkRedundancy(df_modified,savepath,check_columns=None,critical_columns=None):
    total_rows=df_modified.count()
    distinct_rows=df_modified.distinct().count()
    #check for duplicates
    if total_rows>distinct_rows:
        print(f'{total_rows}-{distinct_rows} are duplicate records')
        return 
    
    #check in specified columns

    if check_columns:
        
        dup_check=df_modified.groupBy(check_columns).count().filter("count>1")
        dup_count=dup_check.count()
        if dup_count>0:
            dup_check.show(truncate=False)
            return
    if critical_columns:
        for columns in critical_columns:
            chk=df_modified.filter(col(columns).isNull()).count()
            if chk>0:
                print(f'critical column {columns} has {chk} null values')
                return
    
    df_modified.write.format('delta').mode('append').save(savepath)


chkRedundancy(df_modified,savepath,check_columns=['Departure_Date'],critical_columns=['Economy','PremiumEconomy','Business','First'])

df1=spark.read.format('delta').load(savepath)
df1.count()




