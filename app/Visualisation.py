# Databricks notebook source
# MAGIC %md
# MAGIC ### Movies vs TV Shows trend over the years 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct(country) from default.netflix_final_consumption 

# COMMAND ----------

df = spark.sql("select * from default.netflix_final_consumption")
name = "Ethiopia"
display(df.filter(df.country.contains(name)))


# COMMAND ----------

# getting unique country list

df = spark.sql("select distinct(country) from default.netflix_final_consumption ")
lst = df.rdd.flatMap(lambda x: x).collect()
lst_1 = []
for i in lst:
    if i is not None:
        lst_1.append(i.split(','))
flatList = [element for innerList in lst_1 for element in innerList]

lst_2 = []

for i in flatList:
    if i.strip() != "":
        lst_2.append(i.strip())

list_set = set(lst_2)
unq_lst = (list(list_set))

cntry_lst = []

for i in unq_lst:
    if i.strip() != "":
        cntry_lst.append(i.strip())

cntry_lst.sort()
cntry_lst[:0] = ["All"]

print(cntry_lst)


# COMMAND ----------

IMDb_lst = [9,8,7,6,5,4,3,2,1,0]

# COMMAND ----------


#creating dropdown widget
dbutils.widgets.dropdown("Country", "All", cntry_lst)

# COMMAND ----------

dbutils.widgets.dropdown("IMDb rating greater than", "0", [str(item) for item in IMDb_lst])

# COMMAND ----------

# MAGIC %md
# MAGIC # Movie vs Series trend

# COMMAND ----------

# getting unique country list

df = spark.sql("select distinct(country) from default.netflix_final_consumption ")
lst = df.rdd.flatMap(lambda x: x).collect()
lst_1 = []
for i in lst:
    if i is not None:
        lst_1.append(i.split(','))
flatList = [element for innerList in lst_1 for element in innerList]

lst_2 = []

for i in flatList:
    if i.strip() != "":
        lst_2.append(i.strip())

list_set = set(lst_2)
unq_lst = (list(list_set))

cntry_lst = []

for i in unq_lst:
    if i.strip() != "":
        cntry_lst.append(i.strip())

cntry_lst.sort()
cntry_lst[:0] = ["All"]

#print(cntry_lst)
df = spark.sql("select type, release_year, country,averageRating from default.netflix_final_consumption where release_year between 1970 and 2022")
######################################################################################################################################
countries = dbutils.widgets.get("Country")
imdb = float(dbutils.widgets.get("IMDb rating greater than"))
if countries == "All":  
    countries = cntry_lst
    if imdb == 0:
        display(df)
    else:
        display(df.filter(df.averageRating > imdb))

else:
    if imdb == 0:
        df = df.filter(df.country.contains(countries))
        display(df)
    else:
        df = df.filter((df.country.contains(countries)) & (df.averageRating > imdb))
        display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from (select type, release_year from default.netflix_final_consumption where release_year = "2011" and type = "Movie")

# COMMAND ----------

# MAGIC %md
# MAGIC # ---------------------------------------------------------------------------

# COMMAND ----------

df = spark.sql("select count(*) as cnt, country, type from default.netflix_final_consumption group by country, type ")
display(df)

# COMMAND ----------

from pyspark.sql.functions import split

df = spark.sql("select count(*) as cnt, country, type from default.netflix_final_consumption group by country, type ")

# Split the values column based on the comma delimiter
split_col = split(df['country'], ',')

# Add the split columns to the DataFrame
df = df.withColumn('c1', split_col.getItem(0))
df = df.withColumn('c2', split_col.getItem(1))
df = df.withColumn('c3', split_col.getItem(2))
df = df.withColumn('c4', split_col.getItem(3))
df = df.withColumn('c5', split_col.getItem(4))
df = df.withColumn('c6', split_col.getItem(5))
df = df.withColumn('c7', split_col.getItem(6))
df = df.withColumn('c8', split_col.getItem(7))
df = df.withColumn('c9', split_col.getItem(8))
df = df.withColumn('c10', split_col.getItem(9))
df = df.withColumn('c11', split_col.getItem(10))
df = df.withColumn('c12', split_col.getItem(11))
# Drop the original values column
df = df.drop('country')
m_df = df

# COMMAND ----------

display(m_df)

# COMMAND ----------

df = spark.sql("select distinct(country) from default.netflix_final_consumption ")
lst = df.rdd.flatMap(lambda x: x).collect()
lst_1 = []
for i in lst:
    if i is not None:
        lst_1.append(i.split(','))
flatList = [element for innerList in lst_1 for element in innerList]

lst_2 = []

for i in flatList:
    if i.strip() != "":
        lst_2.append(i.strip())

list_set = set(lst_2)
unq_lst = (list(list_set))

cntry_lst = []

for i in unq_lst:
    if i.strip() != "":
        cntry_lst.append(i.strip())
print(cntry_lst)

# COMMAND ----------

l = []
for i in cntry_lst:
    df = m_df.filter(
        (m_df.c1 == i) | (m_df.c2 == i) | (m_df.c3 == i) | (m_df.c4 == i) | 
        (m_df.c5 == i) | (m_df.c6 == i) | (m_df.c7 == i) | (m_df.c8 == i) | 
        (m_df.c9 == i) | (m_df.c10 == i) | (m_df.c11 == i) | (m_df.c12 == i)
    )
    c = df.agg({'cnt': 'sum'}).collect()[0]['sum(cnt)']
    l.append([i, c])
    print(l)

# COMMAND ----------

dataframe = spark.createDataFrame(l, ["Country","Total_content"]) 

# COMMAND ----------

display(dataframe)

# COMMAND ----------

dataframe.write.mode("overwrite").saveAsTable("default.country_visual_of_content")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.country_visual_of_content

# COMMAND ----------

# MAGIC %md
# MAGIC # Country visualistaion of total contents available

# COMMAND ----------

df = spark.sql("select * from default.country_visual_of_content")
display(df)