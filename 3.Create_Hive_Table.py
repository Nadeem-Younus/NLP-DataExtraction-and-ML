############################# Create Hive Table using spark-shell ###############################
df_exp.createOrReplaceTempView("temptbltxt")

df_exp.createOrReplaceTempView("temptbltxt")

spark.sql("""drop table if exists ##TableName## """)
spark.sql("create table ##TableName## select * from temptbltxt ")	
#################################################################################################