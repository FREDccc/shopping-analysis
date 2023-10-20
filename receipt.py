# Databricks notebook source
receipt_df = spark.read.option('inferSchema', True).json('/mnt/databrickcourse/fetch/receipts.json')

# COMMAND ----------

display(receipt_df)

# COMMAND ----------

receipt_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, explode

# COMMAND ----------

receipt_df_new = receipt_df.select('rewardsReceiptItemList',col('_id.$oid').alias('receipt_id')).withColumn('rewardsReceiptItem',explode(col('rewardsReceiptItemList')))\
                        .drop('rewardsReceiptItemList')

# COMMAND ----------

display(receipt_df_new)

# COMMAND ----------

receipt_df_new.printSchema()

# COMMAND ----------

item_list = receipt_df_new.select(col('receipt_id'),col('rewardsReceiptItem.barcode'),col('rewardsReceiptItem.brandCode'),col('rewardsReceiptItem.competitiveProduct'),col('rewardsReceiptItem.competitorRewardsGroup'),col('rewardsReceiptItem.deleted'),col('rewardsReceiptItem.description'),col('rewardsReceiptItem.discountedItemPrice'),col('rewardsReceiptItem.finalPrice'),col('rewardsReceiptItem.metabriteCampaignId'),col('rewardsReceiptItem.needsFetchReview'),col('rewardsReceiptItem.originalFinalPrice'),col('rewardsReceiptItem.originalMetaBriteBarcode'),col('rewardsReceiptItem.originalMetaBriteDescription'),col('rewardsReceiptItem.originalMetaBriteQuantityPurchased'),col('rewardsReceiptItem.originalReceiptItemText'),col('rewardsReceiptItem.partnerItemId'),col('rewardsReceiptItem.pointsEarned'),col('rewardsReceiptItem.pointsNotAwardedReason'),col('rewardsReceiptItem.pointsPayerId'),col('rewardsReceiptItem.preventTargetGapPoints'),col('rewardsReceiptItem.priceAfterCoupon'),col('rewardsReceiptItem.quantityPurchased'),col('rewardsReceiptItem.rewardsGroup'),col('rewardsReceiptItem.rewardsProductPartnerId'),col('rewardsReceiptItem.targetPrice'),col('rewardsReceiptItem.userFlaggedBarcode'),col('rewardsReceiptItem.userFlaggedDescription'),col('rewardsReceiptItem.userFlaggedNewItem'),col('rewardsReceiptItem.userFlaggedPrice'),col('rewardsReceiptItem.userFlaggedQuantity'))

# COMMAND ----------

display(item_list)

# COMMAND ----------

item_list.createOrReplaceTempView('temp_view_item')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from temp_view_item 
# MAGIC where len(barcode)=12

# COMMAND ----------

display(receipt_df)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime,col,date_format

# COMMAND ----------

receipt_with_column = receipt_df.withColumn('receipt_id',col('_id.$oid'))\
                                .withColumn('CreateDate', from_unixtime(col('createDate.$date')/1000))\
                                .withColumn('DateScanned', from_unixtime(col('dateScanned.$date')/1000))\
                                .withColumn('FinishedDate', from_unixtime(col('finishedDate.$date')/1000))\
                                .withColumn('ModifyDate', from_unixtime(col('modifyDate.$date')/1000))\
                                .withColumn('PointsAwardedDate', from_unixtime(col('pointsAwardedDate.$date')/1000))\
                                .withColumn('PurchaseDate', from_unixtime(col('purchaseDate.$date')/1000))

# COMMAND ----------

receipt_with_column1 = receipt_with_column.withColumn('CreateDate', date_format(col('CreateDate'),'yyyy-MM-dd'))\
                                          .withColumn('DateScanned', date_format(col('DateScanned'),'yyyy-MM-dd'))\
                                          .withColumn('FinishedDate', date_format(col('FinishedDate'),'yyyy-MM-dd'))\
                                          .withColumn('ModifyDate', date_format(col('ModifyDate'),'yyyy-MM-dd'))\
                                           .withColumn('PointsAwardedDate', date_format(col('PointsAwardedDate'),'yyyy-MM-dd'))\
                                           .withColumn('PurchaseDate', date_format(col('PurchaseDate'),'yyyy-MM-dd'))
                                             

# COMMAND ----------

receipt_final = receipt_with_column1.select(col('receipt_id'),col('userId'),col('bonusPointsEarned'),col('bonusPointsEarnedReason'),col('CreateDate'),col('DateScanned'),col('FinishedDate'),col('ModifyDate'),col('PointsAwardedDate'), col('pointsEarned'),col('PurchaseDate'),col('purchasedItemCount'),col('rewardsReceiptStatus'),col('totalSpent'))

# COMMAND ----------

display(receipt_final)

# COMMAND ----------

item_list.write.saveAsTable('shopping.item_list')

# COMMAND ----------

receipt_final.write.saveAsTable('shopping.receipt')

# COMMAND ----------


