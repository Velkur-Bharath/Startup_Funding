# Databricks notebook source
# MAGIC %md
# MAGIC Finding important investors in the Indian Ecosystem

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

funding_schema = StructType(
                    [
                        StructField('Sr.No.',StringType()),
                        StructField('DateTime',StringType()),
                        StructField('StartupName',StringType()),
                        StructField('IndustryVertical',StringType()),
                        StructField('SubVertical',StringType()),
                        StructField('City_Location',StringType()),
                        StructField('InvestorsName',StringType()),
                        StructField('InvestmentType',StringType()),
                        StructField('Amount_in_USD',StringType()),
                        StructField('Remarks',StringType()),
                    ]
)

# COMMAND ----------

# df = spark.sql('SELECT * FROM `hive_metastore`.`default`.`startup_funding`')

funding_data = spark.read.csv('/FileStore/shared_uploads/yashwanth.jonnala@outlook.com/startup_funding.csv',schema=funding_schema,header=True).cache()

# COMMAND ----------

display(funding_data)

# COMMAND ----------

funding_data = funding_data.select('*',
                                   to_date(funding_data.DateTime,'dd-MM-yyyy').alias('Date')
                                  ).drop('DateTime')

# COMMAND ----------

funding_data.filter(funding_data.Date.isNull()).count()

# COMMAND ----------

funding_data.printSchema()

# COMMAND ----------

funding_data = funding_data.select(
                    '*',
                    regexp_replace('Amount_in_USD',r',','').alias('Amount_Invested')
).drop('Amount_in_USD')

# COMMAND ----------

funding_data = funding_data.withColumn('Amount_Invested',col('Amount_Invested').cast(LongType()))

# COMMAND ----------

funding_data.select('Amount_Invested').show()

# COMMAND ----------

funding_data.filter(funding_data.InvestorsName.like('%Und%')).select('InvestorsName').show(truncate=False)

# COMMAND ----------

funding_data = funding_data.withColumn('InvestorsName',regexp_replace(col('InvestorsName'),r'^,',''))

# COMMAND ----------

funding_data.filter(funding_data.InvestorsName.like('%,')).count()

# COMMAND ----------

funding_data = funding_data.withColumn('InvestorsName',initcap(col('InvestorsName')))

# COMMAND ----------

funding_data.filter(funding_data.InvestorsName.like('Undisclosed')).select('InvestorsName').show(truncate=False)

# COMMAND ----------

funding_data = funding_data.withColumn('InvestorsName',regexp_replace(col('InvestorsName'),r'^Undisclosed$','Undisclosed Investors'))

# COMMAND ----------

funding_data.filter(funding_data.InvestorsName.like('%/%')).select('InvestorsName','Amount_Invested').show(truncate=False)

# COMMAND ----------

imp_investors_data = funding_data.groupBy('InvestorsName').agg({'Amount_Invested':'sum','StartupName':'count'})

# COMMAND ----------

display(imp_investors_data.sort(col('count(StartupName)').desc()))

# COMMAND ----------

display(imp_investors_data.sort(col('sum(Amount_Invested)').desc()).limit(5))

# COMMAND ----------

imp_inv_by_amt = imp_investors_data.sort(col('sum(Amount_Invested)').desc()).limit(5)

# COMMAND ----------

imp_inv_by_cnt = imp_investors_data.sort(col('count(StartupName)').desc()).filter('InvestorsName <> "Undisclosed Investors"').filter('InvestorsName <> "Undisclosed Investor"').filter('InvestorsName <> "N/a"').limit(5)

# COMMAND ----------

display(imp_investors_data.sort(col('count(StartupName)').desc()).filter('InvestorsName <> "Undisclosed Investors"').filter('InvestorsName <> "Undisclosed Investor"').filter('InvestorsName <> "N/a"').limit(5))

# COMMAND ----------

imp_investors_data.select(max(col('sum(Amount_Invested)'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Plots

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker

# COMMAND ----------

imp_inv_by_amt.printSchema()
imp_inv_by_cnt.printSchema()

# COMMAND ----------

df1 = imp_inv_by_amt.toPandas()

# COMMAND ----------

df1.head()

# COMMAND ----------

investment = (df1['sum(Amount_Invested)'].astype(float)/1000000000).round(2).astype(str) + 'B'

print(investment)

# COMMAND ----------

fig,ax = plt.subplots(figsize=(15,5))
ax.ticklabel_format(style='plain')
ax.bar(df1['InvestorsName'],df1['sum(Amount_Invested)'])
plt.xlabel('Investors Name')
plt.ylabel('Amount Invested in USD')
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter())
plt.show()

# COMMAND ----------



# COMMAND ----------


