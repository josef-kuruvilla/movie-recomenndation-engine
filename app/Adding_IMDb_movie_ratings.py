# Databricks notebook source

movies_df = spark.sql("select * from default.netflix_titles_withId")
display(movies_df)

# COMMAND ----------

ratings_df = spark.sql("select * from default.title_ratings_votes")
display(ratings_df)

# COMMAND ----------

df_new = movies_df.join(ratings_df, movies_df.IMDb_id == ratings_df.tconst, "left")
df_new = df_new.drop(ratings_df.tconst)
print(df_new.count())
display(df_new)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table default.netflix_final_consumption

# COMMAND ----------

df_new.write.mode("overwrite").saveAsTable("default.netflix_final_consumption")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.netflix_final_consumption 