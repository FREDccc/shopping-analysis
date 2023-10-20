# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

id_schema = StructType(fields=[StructField('$oid',StringType(), True)
                       ])

# COMMAND ----------

cpg_schema = StructType(fields=[StructField('$id',StructType([StructField('$oid',StringType(),True)]),True),
                                StructField('$ref',StringType(), True)
])

# COMMAND ----------

brand_schema = StructType(fields=[StructField('_id', id_schema),
                                  StructField('barcode', StringType(), True),
                                  StructField('brandCode', StringType(), True),
                                  StructField('category', StringType(), True),
                                  StructField('categoryCode', StringType(), True),
                                  StructField('cpg', cpg_schema),
                                  StructField('name', StringType(), True),
                                  StructField('topBrand', StringType(), True)])

# COMMAND ----------

brand_df = spark.read.schema(brand_schema).json('/mnt/databrickcourse/fetch/brands.json')

# COMMAND ----------

brand_df.printSchema()

# COMMAND ----------

display(brand_df)

# COMMAND ----------

from pyspark.sql.functions import col, concat

# COMMAND ----------

brand_df_withColumn = brand_df.withColumn('brand_id',col('_id.$oid'))\
                              .withColumn('cpg_id',col('cpg.$id.$oid'))\
                              .withColumn('cpg_cogs',col('cpg.$ref'))

# COMMAND ----------

display(brand_df_withColumn)

# COMMAND ----------

brand_final = brand_df_withColumn.select(col('brand_id'),col('barcode'),col('brandCode'),col('category'),col('categoryCode'),col('name'),col('topBrand'),col('cpg_id'),col('cpg_cogs'))

# COMMAND ----------

display(brand_final)

# COMMAND ----------

brand_final.write.saveAsTable('shopping.brand')
