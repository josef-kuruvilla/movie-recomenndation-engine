# Databricks notebook source
pip install IMDbPY

# COMMAND ----------

# MAGIC %md
# MAGIC #User ratings (ETL)

# COMMAND ----------

df_1 = spark.sql("select * from default.movie_user_ratings")
display(df_1)

# COMMAND ----------

df_2 = spark.sql("select * from default.movie_id_imdb_map")
df_2 = df_2.withColumnRenamed("movieId", "movieId_new")
display(df_2)

# COMMAND ----------

from pyspark.sql import functions as F
df_joined = df_1.join(df_2, df_1.movieId == df_2.movieId_new,"inner").select(F.col("userId"),F.col("movieId"),F.col("imdbId"),F.col("rating"))
display(df_joined)

df_joined.write.mode("overwrite").saveAsTable("default.user_ratings_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.user_ratings_final

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining user ratings final with Netflix titles

# COMMAND ----------

netflix_df = spark.sql("select * from default.netflix_final_consumption")
display(netflix_df)

# COMMAND ----------

user_df = spark.sql("select * from default.user_ratings_final")
user_df = user_df.withColumnRenamed("rating","IMDb_rating")
display(user_df)

# COMMAND ----------

from pyspark.sql.functions import concat,lit
user_df = user_df.withColumn("tt_IMDb",concat(lit("tt"),user_df.imdbId)).drop("movieId")
display(user_df)

# COMMAND ----------

new_df = netflix_df.join(user_df,user_df.tt_IMDb == netflix_df.IMDb_id,"inner")
display(new_df)

# COMMAND ----------

netflix_user_ratings_df = new_df.select("userId","imdbId","IMDb_rating").withColumnRenamed("IMDb_rating","rating")    
display(netflix_user_ratings_df)

# COMMAND ----------

netflix_user_ratings_df.write.mode("overwrite").saveAsTable("default.netflix_user_IMDb_ratings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.netflix_user_IMDb_ratings