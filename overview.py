# Databricks notebook source
data1 = spark.read.json("/mnt/databrickcourse/raw/2021-03-21/results.json")

# COMMAND ----------

display(data1)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id=dbutils.secrets.get(scope='fetch1-scope',key='fetch-client')
tenant_id=dbutils.secrets.get(scope='fetch1-scope',key='fetch-tenant')
client_secret=dbutils.secrets.get(scope='fetch1-scope',key='fetch1-key')

# COMMAND ----------

# client_id = 'bc1972e5-65af-4aea-b76b-b4ec5d66431a'
# tenant_id = 'fecd7fdb-0eab-44bd-8c31-f220478056e4'
# client_secret='JCe8Q~oJo-c5jhcJylJZdt7MluxR6Vv1USXFpaSw'

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.databrickcourse.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.databrickcourse.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.databrickcourse.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.databrickcourse.dfs.core.windows.net", client_secret)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickcourse.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# display(dbutils.fs.ls("abfss://fetch@databrickcourse.dfs.core.windows.net"))

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://fetch@databrickcourse.dfs.core.windows.net/",
  mount_point = "/mnt/databrickcourse/fetch",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databrickcourse/fetch"))

# COMMAND ----------

brand_raw = spark.read.json('dbfs:/mnt/databrickcourse/fetch/brands.json')

# COMMAND ----------

display(brand_raw)

# COMMAND ----------

user_raw = spark.read.json('/mnt/databrickcourse/fetch/users.json')

# COMMAND ----------

display(user_raw)

# COMMAND ----------

receipt_raw = spark.read.json('/mnt/databrickcourse/fetch/receipts.json')

# COMMAND ----------

display(receipt_raw)
