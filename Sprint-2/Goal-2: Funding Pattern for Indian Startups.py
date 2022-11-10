# Databricks notebook source
# MAGIC %md # Funding Pattern for Indian Startups

# COMMAND ----------

# MAGIC %md ##Importing libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ##Defining schema

# COMMAND ----------

data_schema = StructType(
                    [
                        StructField('Sr.No.',IntegerType()),
                        StructField('DateTime',StringType()),
                        StructField('StartupName',StringType()),
                        StructField('IndustryVertical',StringType()),
                        StructField('SubVertical',StringType()),
                        StructField('City_Location',StringType()),
                        StructField('InvestorsName',StringType()),
                        StructField('InvestmentType',StringType()),
                        StructField('Amount_in_USD',IntegerType()),
                        StructField('Remarks',StringType()),
                    ]
)

# COMMAND ----------

# MAGIC %md ##Importing data

# COMMAND ----------

raw_df = spark.read.csv('/FileStore/tables/startup_funding.csv',schema = data_schema, header = True).cache()

# COMMAND ----------

# MAGIC %md ##Pre-processing the data

# COMMAND ----------

display(raw_df)

# COMMAND ----------

clean_df = raw_df.withColumn('InvestorsName', regexp_replace('InvestorsName', '[uU]ndisclosed.*', 'Undisclosed Investors')).withColumn('City_Location', regexp_replace('City_Location', 'Gurgaon', 'Gurugram')).withColumn('City_Location', regexp_replace('City_Location', '[Bb][ae]ngalore', 'Bengaluru'))

# COMMAND ----------

display(clean_df.filter(clean_df.InvestorsName == 'Undisclosed'))

# COMMAND ----------

display(clean_df.filter(clean_df.City_Location == 'Gurgaon'))

# COMMAND ----------

display(clean_df.filter(clean_df.City_Location == 'Bengalore'))

# COMMAND ----------

# MAGIC %md ##Data visualization

# COMMAND ----------

city_pattern_df = clean_df.filter(col('City_Location') != 'nan').filter(col('Amount_in_USD') > 0).groupBy('City_Location').agg(F.round(avg('Amount_in_USD'), 2).alias('Average Funding'), count(col('City_Location')).alias('Count of Investments')).orderBy(col('Count of Investments').desc(), col('City_Location').asc()).withColumnRenamed('City_Location', 'City')

display(city_pattern_df.limit(10))

# COMMAND ----------

investor_pattern_df = clean_df.filter(col('InvestorsName') != '').filter(col('InvestorsName') != 'N/A').filter(col('Amount_in_USD') > 0).groupBy('InvestorsName').agg(F.round(avg('Amount_in_USD'), 2).alias('Average Funding'), count(col('InvestorsName')).alias('Count of Investments')).orderBy(col('Count of Investments').desc()).limit(10).withColumnRenamed('InvestorsName', 'Investor\'s Name')

investor_pattern_df = investor_pattern_df.orderBy(col('Average Funding').desc())

display(investor_pattern_df)
