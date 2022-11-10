# Databricks notebook source
# MAGIC %md
# MAGIC # 6.Maximum And Minimum Investments Out Of All Startups In India

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing required libraries and defining schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
MaxMin_schema = StructType(
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

# MAGIC %md
# MAGIC #### Importing data

# COMMAND ----------

MaxMin_investments_data = spark.read.csv('/FileStore/tables/startup_funding.csv',schema=MaxMin_schema,header=True).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Pre-processing

# COMMAND ----------

display(MaxMin_investments_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Getting total amount invested by each company

# COMMAND ----------

MaxMin_investments_sum_Amounts = MaxMin_investments_data.groupBy('StartupName').agg({'Amount_in_USD':'sum'}).filter(col("sum(Amount_in_USD)")!=0)

# COMMAND ----------

display(MaxMin_investments_sum_Amounts)

# COMMAND ----------

display(MaxMin_investments_sum_Amounts.sort(col('sum(Amount_in_USD)').desc()))

# COMMAND ----------

display(MaxMin_investments_sum_Amounts.sort(col('sum(Amount_in_USD)')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Adding 2 columns: rankasc(ranking ascending), rankdesc(ranking descending) in sum(Amount_in_USD)) column

# COMMAND ----------

WindowSpec = Window.orderBy(col("sum(Amount_in_USD)").desc())
Max_df = MaxMin_investments_sum_Amounts.withColumn("rankdesc",rank().over(WindowSpec))

# COMMAND ----------

WindowSpec1 = Window.orderBy(col("sum(Amount_in_USD)"))
MaxMin_df = Max_df.withColumn("rankasc",rank().over(WindowSpec1))

# COMMAND ----------

display(MaxMin_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Companies that Invested Maximum Amount

# COMMAND ----------

Max_Investments_df = MaxMin_df.select(['StartupName','sum(Amount_in_USD)']).where(MaxMin_df.rankdesc =="1")
display(Max_Investments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Companies that Invested Minimum Amount

# COMMAND ----------

Min_Investments_df = MaxMin_df.select(['StartupName','sum(Amount_in_USD)']).where(MaxMin_df.rankasc =="1")
display(Min_Investments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Companies that Invested Maximum and Minimum Amounts

# COMMAND ----------

MaxMin_Investments_df = MaxMin_df.select(['StartupName','sum(Amount_in_USD)']).where((MaxMin_df.rankdesc =="1")| (MaxMin_df.rankasc =="1"))

# COMMAND ----------

display(MaxMin_Investments_df.sort(col("sum(Amount_in_USD)").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualising the Tables for Maximum and Minimum Investments

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib as mpl

# COMMAND ----------

MaxMin_Investments_df.printSchema()
Max_Investments_df.printSchema()
Min_Investments_df.printSchema()

# COMMAND ----------

MaxMin_plot_df = MaxMin_Investments_df.toPandas()
MaxMin_plot_df.rename(columns = {'StartupName':'Startup Name','sum(Amount_in_USD)':'Total Amount Invested ($)'}, inplace = True)
Max_plot_df = Max_Investments_df.toPandas()
Max_plot_df.rename(columns = {'StartupName':'Startup Name','sum(Amount_in_USD)':'Total Amount Invested ($)'}, inplace = True)
Min_plot_df = Min_Investments_df.toPandas()
Min_plot_df.rename(columns = {'StartupName':'Startup Name','sum(Amount_in_USD)':'Total Amount Invested ($)'}, inplace = True)

# COMMAND ----------

Max_plot_df.style.set_caption("Maximum Investments").set_properties(**{'background-color': 'cornsilk','border': '1px solid cornsilk',
                          'color': 'brown'}).set_precision(1).set_table_styles([{'selector': 'th','props': [('background-color', 'gainsboro'),('color','dimgray')]}]).hide_index()

# COMMAND ----------

Min_plot_df.style.set_caption("Minimum Investments").set_properties(**{'background-color': 'cornsilk','border': '1px solid cornsilk',
                          'color': 'brown'}).set_precision(1).set_table_styles([{'selector': 'th','props': [('background-color', 'gainsboro'),('color','dimgray')]}]).hide_index()

# COMMAND ----------

MaxMin_plot_df.style.set_caption("Maximum and Minimum Investments").set_properties(**{'background-color': 'cornsilk','border': '1px solid cornsilk',
                          'color': 'brown'}).set_precision(1).set_table_styles([{'selector': 'th','props': [('background-color', 'gainsboro'),('color','dimgray')]}]).hide_index()
