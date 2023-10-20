# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,CharType,VarcharType

# COMMAND ----------

id_schema = StructType(fields=[StructField('$oid',StringType(), True)])

# COMMAND ----------

create_date_schema = StructType(fields=[StructField('$date', DateType(),True)])

# COMMAND ----------

last_login_schema = StructType(fields=[StructField('$date', DateType(),True)])

# COMMAND ----------

user_schema = StructType(fields = [StructField('_id',id_schema),
                                   StructField('active',StringType(), True),
                                   StructField('createDate',create_date_schema),
                                   StructField('lastLogin',last_login_schema),
                                   StructField('role',StringType(), True),
                                   StructField('signUpSource',StringType(), True),
                                   StructField('state',StringType(), True)])

# COMMAND ----------

user_df = spark.read.option('inferSchema', True).json('/mnt/databrickcourse/fetch/users.json')

# COMMAND ----------

display(user_df)

# COMMAND ----------

user_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_unixtime,col,date_format

# COMMAND ----------

user_df_withColumn = user_df.withColumn('CreateDate', from_unixtime(col('createdDate.$date')/1000) )\
                            .withColumn('LastLogin', from_unixtime(col('lastLogin.$date')/1000) )

# COMMAND ----------

display(user_df_withColumn)

# COMMAND ----------

user_df_withColumn1 = user_df_withColumn.withColumn('createdDate1', date_format(col('CreateDate'),'yyyy-MM-dd'))\
                                        .withColumn('loginDate1', date_format(col('LastLogin'),'yyyy-MM-dd'))\
                                        .withColumn('user_id',col('_id.$oid'))

# COMMAND ----------

user_df_final = user_df_withColumn1.select(col('user_id'),col('active'),col('createdDate1'),col('loginDate1'),col('CreateDate'),col('LastLogin'),col('role'),col('signUpSource'),col('state'))

# COMMAND ----------

display(user_df_final)

# COMMAND ----------

user_df_final.write.saveAsTable('shopping.user_df_final')
