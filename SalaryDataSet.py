# Databricks notebook source
##mount the data into notebook

dbutils.fs.mount(
 source="wasbs://datafiles-moni@analyzedata.blob.core.windows.net",
 mount_point="/mnt/projects/",
 extra_configs={'fs.azure.account.key.analyzedata.blob.core.windows.net':'dmkPKTc5OxhPVg/CCptc32HaLFYPaTxthRp1eLSxAtKePai3UaNCfUFiv7+R6GYW0lzztbkESpEo+ASthPao5w=='}
 )

# COMMAND ----------

dbutils.fs.ls('/mnt/projects')

# COMMAND ----------

import numpy as np # linear algebra
import pandas as pd
from matplotlib import pyplot as plt
import plotly.express as px
from matplotlib import pyplot as plt
##store data in df

df=pd.read_csv("/dbfs/mnt/projects/salary_dataset")
df

# COMMAND ----------

##summarise data

df.columns


# COMMAND ----------

df.Salary

# COMMAND ----------

df.rename(columns = {'---':'Company_Name'}, inplace = True)
df.rename(columns = {'Job Title':'Job_Title'}, inplace = True)

# COMMAND ----------

df

# COMMAND ----------

df["Company_Name"].value_counts()

# COMMAND ----------

df.Job_Title.unique()

# COMMAND ----------

df_new=spark.createDataFrame(df1)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

result = df_new.groupBy("Company Name", "Job Title", "Salaries Reported", "Location", "Salary") \
            .agg(count("*").alias("count")) \
            .filter("count > 1")
display(result)

# COMMAND ----------

#Checking duplicates

duplicate = df[df.duplicated()]
duplicate

# COMMAND ----------

#drop duplicates
df=df.drop_duplicates()
display(df)

# COMMAND ----------

#checking null
bool_series = pd.isnull(df["Company_Name"])
df[bool_series]

# COMMAND ----------

#Removing Null Values

df=df.dropna()
display(df)

# COMMAND ----------

#Cleaning Salary Colum
# converting Salary column String to Int
df['Currency'] = df['Salary'].str.extract(r'(\D+)')
df['Period'] = df['Salary'].str.slice(-2)
df['Amount'] = df['Salary'].str.replace(',','').str.extract(r'(\d+)').astype(float)

# COMMAND ----------

df['Currency'].unique()

# COMMAND ----------

df['Period'].unique()

# COMMAND ----------

df['Amount'].dtype

# COMMAND ----------

df

# COMMAND ----------

# convert the curreny into Rupee
def convert_currency(dataframe):
    dataframe['Salary_INR'] = dataframe['Amount']
    dataframe['Salary_USD'] = dataframe['Amount']
    # Converting salary into Rupee
    dataframe['Salary_INR'].loc[dataframe['Currency'] == '$'] = dataframe['Salary_INR']*82.73  
    dataframe['Salary_INR'].loc[dataframe['Currency'] == '£'] = dataframe['Salary_INR']*100.47
    dataframe['Salary_INR'].loc[dataframe['Currency'] == 'AFN '] = dataframe['Salary_INR']*0.95
    dataframe['Salary_INR'].loc[dataframe['Currency'] == '₹'] = dataframe['Salary_INR']

    #Converting Salary into USD
    dataframe['Salary_USD'].loc[dataframe['Currency'] == '₹'] = dataframe['Salary_USD']*.012 
    dataframe['Salary_USD'].loc[dataframe['Currency'] == '£'] = dataframe['Salary_USD']* 1.26
    dataframe['Salary_USD'].loc[dataframe['Currency'] == 'AFN '] = dataframe['Salary_USD']*0.012
    dataframe['Salary_USD'].loc[dataframe['Currency'] == '$'] = dataframe['Salary_USD']
    # converting hourly salary to monthly
    hours_work_per_week = 40
    dataframe['Salary_INR'].loc[df['Period'] == 'hr'] = dataframe['Salary_INR'] * hours_work_per_week * 4
    dataframe['Period'].loc[dataframe['Period'] == 'hr'] = 'mo'
    
    # converting monthly salary to yearly
    dataframe['Salary_INR'].loc[dataframe['Period'] == 'mo'] = dataframe['Salary_INR']*12

    return dataframe


# COMMAND ----------

final_dataset = convert_currency(df)


# COMMAND ----------

display(final_dataset)

# COMMAND ----------

df2=final_dataset.loc[final_dataset['Company_Name'] == 'Tiger Analytics']
display(df2)

# COMMAND ----------

df1=final_dataset.loc[final_dataset['Job_Title'] == 'Data Analyst']
display(df1)


# COMMAND ----------

grouped_loc = df1.groupby('Company_Name')[['Salary_INR']].agg('mean').reset_index()

fig = px.bar(grouped_loc, x="Company_Name", y="Salary_INR", text="Salary_INR", title="Average Yearly Salary in INR by Job Title",
             height=1200,width=1600)
fig.update_traces(textposition="outside", texttemplate='%{text:.3s}')
fig.update_layout(title={'x':0.7, 'xanchor': 'center',})
fig.show()

# COMMAND ----------

df3=final_dataset.loc[final_dataset['Job_Title'] == 'Data Analyst']
display(df3)
maxDF = df3.groupby('Job_Title')[['Salary_INR']].agg('max').reset_index()
minDF=df3.groupby('Job_Title')[['Salary_INR']].agg('min').reset_index()
df4 = pd.merge(maxDF, minDF, on='Job_Title')
display(df4)
ax = df4.plot(kind='bar', x='Job_Title', logy=True, rot=0)
for container in ax.containers:
    ax.bar_label(container)
plt.show()


# COMMAND ----------

df3=final_dataset.loc[final_dataset['Job_Title'] == 'Machine Learning Engineer']
display(df3)
maxDF = df3.groupby('Job_Title')[['Salary_INR']].agg('max').reset_index()
minDF=df3.groupby('Job_Title')[['Salary_INR']].agg('min').reset_index()
df4 = pd.merge(maxDF, minDF, on='Job_Title')
display(df4)
ax = df4.plot(kind='bar', x='Job_Title', logy=True, rot=0)
for container in ax.containers:
    ax.bar_label(container)
plt.show()

# COMMAND ----------

grouped_loc = final_dataset.groupby('Location')[['Salary_INR']].agg('mean').reset_index()

fig = px.bar(grouped_loc, x="Location", y="Salary_INR", text="Salary_INR", title="Average Yearly Salary in INR by Location",
             height=500,width=600)
fig.update_traces(textposition="outside", texttemplate='%{text:.3s}')
fig.update_layout(title={'x':0.5, 'xanchor': 'center',})
fig.show()

# COMMAND ----------

grouped_loc = final_dataset.groupby('Job_Title')[['Salary_INR']].agg('mean').reset_index()

fig = px.bar(grouped_loc, x="Job_Title", y="Salary_INR", text="Salary_INR", title="Average Yearly Salary in INR by Job Title",
             height=1200,width=1600)
fig.update_traces(textposition="outside", texttemplate='%{text:.3s}')
fig.update_layout(title={'x':0.7, 'xanchor': 'center',})
fig.show()

# COMMAND ----------



# COMMAND ----------

grouped_job = final_dataset.groupby(['Location','Job_Title'])[['Salary_INR']].agg('mean').reset_index()

fig = px.bar(grouped_job.sort_values('Salary_INR', ascending=False), x='Salary_INR', y='Job_Title', color='Location',
             text='Salary_INR', title="Average Yearly Salary in INR for different Job Title based on Location",
             orientation='h',color_discrete_sequence= px.colors.cyclical.Phase ,height=800)
fig.update_traces(textposition="auto", texttemplate='%{text:.3s}',textfont=dict(color='white', size=10))
fig.update_layout(title={'x':0.5, 'xanchor': 'center',})
fig.update_xaxes(nticks=10)
fig.show()


# COMMAND ----------

fig = px.pie(final_dataset, values='Salaries Reported', names='Location')
fig.show()

# COMMAND ----------

final_dataset['Field'] = 'Data'

data = final_dataset['Job_Title'].str.contains('Data')
science = final_dataset['Job_Title'].str.contains('Science')
scientist = final_dataset['Job_Title'].str.contains('Scientist')
machine = final_dataset['Job_Title'].str.contains('Machine')
learning = final_dataset['Job_Title'].str.contains('Learning')
engineer = final_dataset['Job_Title'].str.contains('Engineer')
engineering = final_dataset['Job_Title'].str.contains('Engineering')
analyst = final_dataset['Job_Title'].str.contains('Analyst')

final_dataset['Field'].loc[data & (science | scientist) & ~machine] = 'Data Science'
final_dataset['Field'].loc[machine & learning & ~(data & scientist)] = 'Machine Learning'
final_dataset['Field'].loc[data & analyst & ~(machine | scientist)] = 'Data Analyst'
final_dataset['Field'].loc[data & (engineer | engineering)] = 'Data Engineering'

px.histogram(final_dataset,x='Field',y='Salary_INR',histfunc='avg',color='Location',barmode='group')

# COMMAND ----------

#salary_array = final_dataset[["Salary_INR"]].to_numpy() 
salary_list = final_dataset["Salary_INR"].tolist()
print (salary_list)
print(np.max(salary_list))

# COMMAND ----------

  
mean_Salary = np.mean(salary_list)
print(mean_Salary)


# COMMAND ----------

plt.hist(salary_list,edgecolor='black',color='purple',log=True)
plt.axvline(mean_Salary,color='red',lw=3,label=f'Average (Mean) Salary: ${mean_Salary:,.0f}')
plt.xlim(xmin=0, xmax = 200000000)
plt.xlabel('Salary ')
plt.ylabel('Count')
plt.title('Salary in INR (Positive Skew)')
plt.legend();


# COMMAND ----------


salary_listUSD = final_dataset["Salary_USD"].tolist()
mean_SalaryUSD = np.mean(salary_listUSD)
plt.hist(salary_listUSD,edgecolor='black',color='purple',log=True)
plt.axvline(mean_SalaryUSD,color='red',lw=3,label=f'Average (Mean) Salary: ${mean_SalaryUSD:,.0f}')
plt.xlabel('House Price')
plt.ylabel('Count')
plt.title('House Prices (Positive Skew)')
plt.legend();
