# Databricks notebook source
pip install IMDbPY

# COMMAND ----------

# setting your user ID
rating_df = spark.sql("select userId, imdbId, rating from default.netflix_user_IMDb_ratings")

user_cnt = rating_df.select("userId").distinct().count()

user_id_new = user_cnt + 1
print("your user ID is {}".format(user_id_new))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Finding the movie_ID and checking in the user list

# COMMAND ----------

movies selected = Inception, Django Unchained, The Conjuring

# COMMAND ----------

import imdb
ia = imdb.IMDb()

search = ia.search_movie("Inception")
movie_id = search[0].movieID
movie_name = ia.get_movie(movie_id)

c = rating_df.filter(rating_df.imdbId == movie_id).count()  

if c == 0:  
    print("Movie {} not found in the ratings table, pick another movie please".format(movie_name))    
else:    
    print("Movie {} found in the ratings table".format(movie_name))
    print("IMDB ID: " + movie_id)   

# COMMAND ----------

# MAGIC %md
# MAGIC ### **-----------------------------  INPUT YOUR RATINGS HERE  -----------------------------**

# COMMAND ----------

columns = ['userId','imdbId','rating']

Rating_1 = spark.createDataFrame([[user_id_new,1375666,4]],columns)
Rating_2 = spark.createDataFrame([[user_id_new,1853728,5]],columns)
Rating_3 = spark.createDataFrame([[user_id_new,1457767,3]],columns)
Rating_4 = spark.createDataFrame([[user_id_new,3460252,4.5]],columns)
Rating_5 = spark.createDataFrame([[user_id_new,1099212,2]],columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **--------------------------------------------------------------------------------------------------------------**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Updating the user ratings table

# COMMAND ----------

df_new = rating_df.union(Rating_1).union(Rating_2).union(Rating_3).union(Rating_4).union(Rating_5)
df_new.count()
df_new.write.mode("overwrite").saveAsTable("default.user_ratings_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **---------------------------------  Get your recommendation here  ---------------------------------**

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F
import imdb

df = spark.sql("select * from default.user_ratings_final")
ia = imdb.IMDb()

als = ALS()
(als.setSeed(273)
    .setColdStartStrategy("drop")
    .setUserCol("userId")
    .setItemCol("imdbId")
    .setRatingCol("rating"))

recmndn_df = als.fit(df).recommendForAllUsers(10)

my_list = recmndn_df.filter(recmndn_df.userId == user_id_new).select(F.col("recommendations")).rdd.flatMap(lambda x: x).collect()
recmdn_lst = []
for i in range(0,10):
    recmdn_lst.append(my_list[0][i]['imdbId'])
#print(recmdn_lst)

print("top 10 movies recommended for user {user_id_new}: ".format(user_id_new = user_id_new))

for i in recmdn_lst:
    movie_name = ia.get_movie(i)
    print(movie_name)