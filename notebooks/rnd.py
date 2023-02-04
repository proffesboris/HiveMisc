# Databricks notebook source
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

tf_data.write.csv("s3a://proffboris1993-myrndbucket/result")

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

df.show()


# After you have applied transformations to the data, you can use
# the data source API to write the data back to another table

table_name_for_write = "public.category_after"


df.createOrReplaceTempView("table11")

print("-----make transformation----------")

df = spark.sql("select * from table11 where catid < 7")

df.show()


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

