# Databricks notebook source
# MAGIC %md
# MAGIC Importing required libraries and csv file.

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
df1 = pd.read_csv("/dbfs/FileStore/shared_uploads/akshata.utekar786@outlook.com/startup_funding.csv")

# COMMAND ----------

df1.head()

# COMMAND ----------

df1.shape

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if there are any null values.

# COMMAND ----------

df1.isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Handling null values

# COMMAND ----------

df1.fillna(0,inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Data Cleaning

# COMMAND ----------

for data in df1['Industry Vertical']:
    print(data)

# COMMAND ----------

df1['Industry Vertical'] = df1['Industry Vertical'].replace(["E-Commerce","E-commerce","eCommerce","Ecommerce","eCommece","ecommerce","eCommerce returns etailer","ECommerce Website Creation SAAS platform","ECommerce platform solutions","eCommerce Product Search Engine","Ecommerce Discount & Cashback coupons platform","ecommerce related software product platform","Ecommerce Delivery locker services","ECommerce Brands\\xe2\\x80\\x99 Full Service Agency","ECommerce Data Analytics Platform","Ecommerce Product recommendation platform","Ecommerce Marketplace","ECommerce Logistics provider","Ecommerce Marketing Software Platform","eCommerce platform","Ecommerce Logistics"], "ECommerce")

# COMMAND ----------

df1['Industry Vertical'] = df1["Industry Vertical"].replace("Tech","Technology")

# COMMAND ----------

df1['Industry Vertical'] = df1['Industry Vertical'].replace(["Health and Wellness","Health and wellness","Health Care"],"Healthcare")

# COMMAND ----------

df1['Industry Vertical'] = df1['Industry Vertical'].replace(0,"Other")

# COMMAND ----------

# MAGIC %md
# MAGIC Industries Which are highly favoured by the Investors.

# COMMAND ----------

df1['Industry Vertical'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizing top 5 industries which are most likely to be funded.

# COMMAND ----------

plt.figure(figsize=(8,6))
sns.barplot(x=df1['Industry Vertical'].value_counts()[:5].keys(),y=df1['Industry Vertical'].value_counts()[:5],data=df1,palette="rocket")
plt.title('Top 5 Indsutries most likely to be funded',fontsize=15)
plt.xlabel('Industry')
plt.ylabel('Number of Funds received')
plt.show()
