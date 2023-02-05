# Databricks notebook source
dbutils.widgets.text("greeting", "world", "Greeting")
greeting = dbutils.widgets.get("greeting")

# COMMAND ----------

print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### S3 test

# COMMAND ----------

data = spark.read.option("header", "true").csv("s3a://proffboris1993-myrndbucket/Affiliate - Affiliates approved.csv")

data.createOrReplaceTempView("table1")

tf_data = spark.sql("select * from table1 where Code = 'CAE'")

tf_data.show()

tf_data.write.mode("overwrite").csv("s3a://proffboris1993-myrndbucket/result")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Redshift test

# COMMAND ----------

table_name = "public.category"
username = "admin"
password = "MyLife4211"


# Read data from a table
df = (spark.read
  .format("redshift")
  .option("dbtable", table_name)
  .option("tempdir", "s3a://proffboris1993-myrndbucket/temp/")
  .option("url", "jdbc:redshift://redshift-cluster-1.cgcmuxqmjdx2.us-east-1.redshift.amazonaws.com:5439/dev")
  .option("user", username)
  .option("password", password)
  .option("forward_spark_s3_credentials", True)
  .load()
)

df2 = df


# After you have applied transformations to the data, you can use
# the data source API to write the data back to another table

table_name_for_write = "public.category_after"


df.createOrReplaceTempView("table11")

print("-----make transformation----------")

df = spark.sql("select * from table11 where catid < 7")


(df.write
  .format("redshift")
  .option("dbtable", table_name_for_write)
  .option("tempdir", "s3a://proffboris1993-myrndbucket/temp/")
  .option("url", "jdbc:redshift://redshift-cluster-1.cgcmuxqmjdx2.us-east-1.redshift.amazonaws.com:5439/dev")
  .option("user", username)
  .option("password", password)
  .option("forward_spark_s3_credentials", True)
  .mode("overwrite")
  .save()
)




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Redshift SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS redshift_table;
# MAGIC CREATE TABLE redshift_table
# MAGIC USING redshift
# MAGIC OPTIONS (
# MAGIC   dbtable 'public.category',
# MAGIC   tempdir 's3a://proffboris1993-myrndbucket/temp/',
# MAGIC   url 'jdbc:redshift://redshift-cluster-1.cgcmuxqmjdx2.us-east-1.redshift.amazonaws.com:5439/dev',
# MAGIC   user 'admin',
# MAGIC   password 'MyLife4211',
# MAGIC   forward_spark_s3_credentials 'true'
# MAGIC );
# MAGIC SELECT * FROM redshift_table;

# COMMAND ----------

# MAGIC %md
# MAGIC #### BI

# COMMAND ----------

display(df2)

# COMMAND ----------

df3 = spark.read.csv("s3a://proffboris1993-myrndbucket/src_ori/dev_staging_amdelivery_delivery_order.csv")

df3.createOrReplaceTempView("table1")

df3 = spark.sql("select _c20 as type1, _c31 as region, _c61 as type2 from table1")



display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### AVRO DATA

# COMMAND ----------


# Create a DataFrame from a specified directory
df = spark.read.format("avro").load("s3a://proffboris1993-myrndbucket/avro_data/episodes.avro")

df.printSchema()

#  Saves the subset of the Avro records read in
subset = df.where("doctor > 5")

subset.show()

subset.write.format("avro").mode("overwrite").save("s3a://proffboris1993-myrndbucket/avro_data/result/")

# COMMAND ----------

