# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze layer tables

# COMMAND ----------

# MAGIC %md
# MAGIC 1. default.netflix_titles (given)
# MAGIC 2. default.title_ratings_votes
# MAGIC 3. default.title_crew
# MAGIC 4. default.director_names
# MAGIC 5. default.title_imdb_id
# MAGIC 6. default.movie_user_ratings
# MAGIC 7. default.movie_id_imdb_map
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer tables

# COMMAND ----------

# MAGIC %md
# MAGIC 1. default.netflix_titles_withId
# MAGIC 2. default.user_ratings_final

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer tables

# COMMAND ----------

# MAGIC %md
# MAGIC 1. default.netflix_final_consumption
# MAGIC 2. default.country_visual_of_content 
# MAGIC 3. default.netflix_user_IMDb_ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.netflix_titles

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.movie_user_ratings

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.netflix_final_consumption

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.netflix_final_consumption

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.netflix_titles limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.title_ratings_votes limit 10

# COMMAND ----------

# MAGIC  
# MAGIC %sql
# MAGIC Select * from default.title_crew limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.director_names limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.director_names limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.title_imdb_id limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.movie_user_ratings limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.movie_id_imdb_map limit 10 

# COMMAND ----------

#silver

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.netflix_titles_withId limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.user_ratings_final limit 10 

# COMMAND ----------

##gold

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.netflix_final_consumption limit 10 

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.country_visual_of_content limit 10 

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC Select * from default.netflix_user_IMDb_ratings limit 10 