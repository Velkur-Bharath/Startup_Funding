# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# COMMAND ----------

data = pd.read_csv("/dbfs/FileStore/shared_uploads/akshata.utekar786@outlook.com/startup_funding.csv")

# COMMAND ----------

data.head()

# COMMAND ----------

data.isnull().sum()

# COMMAND ----------

null_vals = data.isnull().sum()[3:]

plt.figure(figsize = (15, 5))
sns.barplot(null_vals.index, null_vals, log = True)

plt.title("Count of Null Values", fontsize = 20)
plt.xlabel('Features', fontsize = 20)
plt.ylabel('Count', fontsize = 20);

# COMMAND ----------

data['Investors Name'] = data['Investors Name'].fillna("Anonymous")
data.rename(columns = {'InvestmentnType': 'Investment Type'}, inplace = True)
data.head()

# COMMAND ----------

for i in data['Investors Name']:
    print(i)

# COMMAND ----------

df = data[['Startup Name', 'Investors Name', 'Investment Type', 'Amount in USD']]
df.head(10)

# COMMAND ----------

df.describe()

# COMMAND ----------

df1 = df.copy()
df1.dropna(subset = ["Amount in USD"], inplace=True)
df1.loc[(df1["Amount in USD"] == "undisclosed") | (df1["Amount in USD"] == "Undisclosed") | 
       (df1["Amount in USD"] == "unknown"), "Amount in USD"] = "0"
df1['Amount in USD'] = df1['Amount in USD'].str.replace('+', '', regex = True)
df1['Amount in USD'] = df1['Amount in USD'].str.replace('\\', '', regex = True)
df1['Amount in USD'] = df1['Amount in USD'].str.replace('xc2xa0', '', regex = True)
df1.drop(df1.loc[(df1["Amount in USD"] == "N/A")].index, inplace = True)
df1.reset_index(drop = True, inplace = True)

def f(s):
    m = s.split(",")
    return "".join(m)
df1["Amount in USD"] = df1["Amount in USD"].apply(f)
df1["Amount in USD"] = pd.to_numeric(df1["Amount in USD"])

# COMMAND ----------

df1.head()

# COMMAND ----------

df1.isnull().sum()

# COMMAND ----------

df1 = df1.dropna(subset = ['Investment Type'])
df1.isnull().sum()

# COMMAND ----------

angel = df1.loc[df1['Investment Type'].str.contains("Equity | Angel | Debt | Seed")]
angel.reset_index(drop = True, inplace = True)
angel.head()

# COMMAND ----------

c = angel.copy()
d = {}
updatelist = []
invest = np.array(list(c["Investors Name"]))
for i in range(len(invest)):
    if invest[i] != "" or invesr[i] != " ": 
        if "," in invest[i]:
          sublist = invest[i].strip().split(",")
          for i in sublist:
            if i != "" or i != ' ':
                updatelist.append(i.strip())
        else:
          updatelist.append(invest[i].strip())
        
for i in updatelist:
    if i in d:
        d[i]+=1
    else:
        d[i]=1
        
d = dict(sorted(d.items(), key = lambda x:x[1], reverse = True))
print(d)

# COMMAND ----------

dict_data = pd.DataFrame.from_dict(d, columns = ['Investments Count'], orient = 'index')
dict_data

# COMMAND ----------

# Angel Investors of India by Number of Investments

x = ['Anonymous', 'Nexus Venture Partners', 'LetsVenture', 'Accion Venture Lab', 'Ankur Capital', 
     'Axilor Ventures', 'Tiger Global Management', 'Ant Financial', 'TIW Private Equity', 'Qatar Investment Authority']
y = [d['Anonymous'], d['Nexus Venture Partners'], d['LetsVenture'], d['Accion Venture Lab'], d['Ankur Capital'], 
     d['Axilor Ventures'], d['Tiger Global Management'], d['Ant Financial'], d['TIW Private Equity'], 
     d['Qatar Investment Authority']]
c = ['red', 'yellow', 'black', 'blue', 'orange', 'green', 'violet', 'pink', 'cyan', 'purple']

plt.bar(x, y, color = c)
plt.title("Count of Investments", fontsize = 20)
plt.xlabel('Investors', fontsize = 20)
plt.ylabel('Count', fontsize = 20)
plt.xticks(rotation = 90)
plt.show()

# COMMAND ----------

# Angel Investors with Amount of Investment

angel['Amount in USD'].dtypes

# COMMAND ----------

usd = angel[['Investors Name', 'Amount in USD']]
a = usd.sort_values(by = 'Amount in USD', ascending = False)
a.head(10)

# COMMAND ----------

sum_amt = a.groupby(['Investors Name']).agg({'Amount in USD': sum}).sort_values(by = 'Amount in USD').tail(10)
amount = pd.DataFrame(sum_amt).reset_index()
amount

# COMMAND ----------

# sum_amt = amount.groupby(['Investors Name']).sum()
# sum_amt.plot(kind = 'pie', y = 'Amount in USD', autopct = '%1.0f%%')
plt.figure(figsize = (15, 10))
plt.pie(amount['Amount in USD'], labels = amount['Investors Name'], 
        autopct = '%1.0f%%', explode = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0.1))
plt.title("Amount Invested", fontsize = 20)
plt.axis('equal')
plt.show()

# COMMAND ----------


