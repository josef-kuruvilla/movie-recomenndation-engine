# Databricks notebook source
# MAGIC %md
# MAGIC # Getting the director name for movie id

# COMMAND ----------

crew_id_df = spark.sql("select * from default.title_crew ")
display(crew_id_df)

# COMMAND ----------

crew_name_df = spark.sql("select * from default.director_names")
#display(crew_name_df)

# COMMAND ----------

df_joined = crew_id_df.join(crew_name_df, crew_id_df.directors == crew_name_df.nconst,"inner")
movie_director_df = df_joined.select("tconst", "directors", "primaryName")        
display(movie_director_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Available datasets

# COMMAND ----------

#id, imdb rating

rating_df = spark.sql("select * from default.title_ratings_votes")
display(rating_df)

# COMMAND ----------

#id, title name

from pyspark.sql.functions import when

link_df_1 = spark.sql("select * from default.title_imdb_id")
link_df_2 = link_df_1.filter(link_df_1.titleType.isin(['tvSeries', 'movie','tvMiniSeries','tvMovie'])).distinct()

link_df = link_df_2.withColumn("titleType", when(link_df_2.titleType =='tvSeries', 'TV Show').when(link_df_2.titleType =='movie', 'Movie').when(link_df_2.titleType =='tvMiniSeries', 'TV Show').when(link_df_2.titleType =='tvMovie', 'Movie'))
display(link_df)

# COMMAND ----------

# netflix movies (primary dataset given)

netflix_df = spark.sql("select * from default.netflix_titles where type not in ('William Wyler') and type is not null")
netflix_df = netflix_df.distinct()
display(netflix_df.filter(netflix_df.title == 'Hello Ninja'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining the 4 dataframes

# COMMAND ----------

# joining link df with director names

link_df_withDirector = link_df.join(movie_director_df,'tconst',how='inner')
link_df_withDirector = link_df_withDirector.select('tconst','titleType','primaryTitle','startYear','directors','primaryName')
link_df_withDirector = link_df_withDirector.withColumnRenamed('primaryName','director_link')
link_df_withDirector = link_df_withDirector.withColumnRenamed('directors','director_id')
display(link_df_withDirector)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining link_df_withDirector with Netflix_titles

# COMMAND ----------

final_df = link_df_withDirector.join(netflix_df, (link_df_withDirector.primaryTitle == netflix_df.title) & (link_df_withDirector.director_link == netflix_df.director), how='right')
display(final_df)

# COMMAND ----------

from pyspark.sql.functions import col

df_1 = final_df.filter(~ col("tconst").isNull())
display(df_1)

# COMMAND ----------

error_df = final_df.filter(col("tconst").isNull())
display(error_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the error records

# COMMAND ----------

# First, perform the aggregation to find titles with a count of 1
titles_with_single_occurrence = link_df_withDirector.groupBy('primaryTitle') \
    .count() \
    .filter("count = 1") \
    .select('primaryTitle')

# Then, join this result back to the original DataFrame to filter the rows
filtered_df = link_df_withDirector.join(titles_with_single_occurrence, 'primaryTitle')

# Display the result
display(filtered_df)

# COMMAND ----------

df_2 = error_df.join(filtered_df, error_df.title == filtered_df.primaryTitle,'left')
df_2 = df_2.drop(error_df.tconst)
df_2 = df_2.drop(error_df.titleType)
df_2 = df_2.drop(error_df.primaryTitle)
df_2 = df_2.drop(error_df.startYear)
df_2 = df_2.drop(error_df.director_id)
df_2 = df_2.drop(error_df.director_link)

df_2 = df_2.select(df_2.tconst,df_2.titleType,df_2.primaryTitle,df_2.startYear,df_2.director_id,df_2.director_link,df_2.show_id,df_2.type,df_2.title,df_2.director,df_2.cast,df_2.country,df_2.date_added,df_2.release_year,df_2.rating,df_2.duration,df_2.listed_in,df_2.description)

display(df_2)

# COMMAND ----------

df_union = df_1.union(df_2)
print(df_union.count())
display(df_union)

# COMMAND ----------

df_union = df_union.select('tconst','show_id','type','title','director','cast','country','date_added','release_year','rating','duration','listed_in','description')
df_union = df_union.withColumnRenamed('tconst','IMDb_id')
display(df_union)

# COMMAND ----------

df_union = df_union.distinct()
display(df_union)

# COMMAND ----------

df_union.write.mode('overwrite').saveAsTable('default.netflix_titles_withId')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.netflix_titles_withId