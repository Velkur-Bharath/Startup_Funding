# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# COMMAND ----------

data = pd.read_csv("/dbfs/FileStore/shared_uploads/akshata.utekar786@outlook.com/startup_funding.csv")
updated_data = data[['Date dd/mm/yyyy','Amount in USD']]

# COMMAND ----------

updated_data['Amount in USD'] = updated_data['Amount in USD'].fillna(0)
updated_data['Amount in USD'] = updated_data['Amount in USD'].str.replace(',','')
updated_data['Amount in USD'] = updated_data['Amount in USD'].astype(float)



# COMMAND ----------

updated_data['Amount in USD'] = updated_data['Amount in USD'].fillna(0)

# COMMAND ----------

final_data = updated_data[['Date dd/mm/yyyy','Amount in USD']]


# COMMAND ----------

final_data['Date dd/mm/yyyy'] = pd.to_datetime(final_data['Date dd/mm/yyyy'],errors = 'coerce')

# COMMAND ----------

final_data = final_data.drop(109)

# COMMAND ----------

final_data = final_data[final_data["Amount in USD"].str.contains("xc2")==False]
final_data = final_data[final_data["Amount in USD"].str.contains("un")==False]
final_data = final_data[final_data["Amount in USD"].str.contains("Un")==False]


# COMMAND ----------

final_data.loc[data["Amount in USD"] == '14342000+']

# COMMAND ----------

with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    print(final_data)

# COMMAND ----------

final_data.head()

# COMMAND ----------

final_data['Amount in USD'] = final_data['Amount in USD'].astype(float)

# COMMAND ----------

final_data.rename(columns = {'Date dd/mm/yyyy':'Timeline'}, inplace = True)

# COMMAND ----------

final_data.groupby(final_data['Timeline'].dt.to_period('Q'))['Amount in USD'].sum()

# COMMAND ----------

final_data.groupby(final_data['Timeline'].dt.to_period('1Y'))['Amount in USD'].sum().plot.pie( x='Quarters', y='Amount spent in USD', figsize=(25, 10))

# COMMAND ----------

final_data.groupby(final_data['Timeline'].dt.to_period('q'))['Amount in USD'].sum().plot.bar(x='names', rot=0, figsize=(25, 10))

# COMMAND ----------

print(quarter_data['Date dd/mm/yyyy'])

# COMMAND ----------

x = updated_data[""]
plt.bar(x, y)
plt.title("Count of Null Values", fontsize = 20)
plt.xlabel('Features', fontsize = 20)
plt.ylabel('Count', fontsize = 20);

# COMMAND ----------

final_data.groupby(final_data['Timeline'].dt.to_period('1Y'))['Amount in USD'].sum()

