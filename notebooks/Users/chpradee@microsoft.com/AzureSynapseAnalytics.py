# Databricks notebook source
# Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.chepra.blob.core.windows.net",
  "gv7nVISerl8wbK9mPGm8TC3CQIEjV3Z5dQ9sHjxnZ7DnUPlQyYnd1zUayGWoyE0CU7dWPTAqaXhKyb9hldlOiA==")

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://cheprasynapse.sql.azuresynapse.net:1433;database=chepra;user=chepra@cheprasynapse;password=KTM.ktm@12345;encrypt=true;trustServerCertificate=false;hostNameInCertificate=cheprasynapse.sql.azuresynapse.net;loginTimeout=30;") \
  .option("tempDir", "wasbs://sampledata@chepra.blob.core.windows.net/temp") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "trip") \
  .load()

# COMMAND ----------

# Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.chepra.blob.core.windows.net",
  "gv7nVISerl8wbK9mPGm8TC3CQIEjV3Z5dQ9sHjxnZ7DnUPlQyYnd1zUayGWoyE0CU7dWPTAqaXhKyb9hldlOiA==")

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://cheprasynapse.sql.azuresynapse.net:1433;database=chepra;user=chepra@cheprasynapse;password=KTM.ktm@12345;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;") \
  .option("tempDir", "wasbs://sampledata@chepra.blob.core.windows.net/temp") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "trip") \
  .load()

# COMMAND ----------

# Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.chepra.blob.core.windows.net",
  "gv7nVISerl8wbK9mPGm8TC3CQIEjV3Z5dQ9sHjxnZ7DnUPlQyYnd1zUayGWoyE0CU7dWPTAqaXhKyb9hldlOiA==")

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://cheprasynapse.sql.azuresynapse.net:1433;database=chepra;user=chepra@cheprasynapse;password=KTM.ktm@12345;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;authentication=ActiveDirectoryPassword") \
  .option("tempDir", "wasbs://sampledata@chepra.blob.core.windows.net/temp") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "trip") \
  .load()

# COMMAND ----------

# Set up the Blob storage account access key in the notebook session conf.
spark.conf.set(
  "fs.azure.account.key.chepra.blob.core.windows.net",
  "gv7nVISerl8wbK9mPGm8TC3CQIEjV3Z5dQ9sHjxnZ7DnUPlQyYnd1zUayGWoyE0CU7dWPTAqaXhKyb9hldlOiA==")

# Get some data from an Azure Synapse table.
df = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", "jdbc:sqlserver://cheprasynapse.sql.azuresynapse.net:1433;database=chepra;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;Authentication=ActiveDirectoryIntegrated") \
  .option("tempDir", "wasbs://sampledata@chepra.blob.core.windows.net/temp") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "trip") \
  .load()

# COMMAND ----------

spark.conf.set("fs.azure.account.key.chepragen2.dfs.core.windows.net", "48T0fl5dEP49Na6sV0RhGNKbZzVmjXLIAo8057VTs11kMYcQs3o70JLu0aDxro2WoD2wwOSMcHUyMY8f5oGd6Q==");

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.format("com.databricks.spark.sqldw")
# MAGIC .option("url", "jdbc:sqlserver://cheprasynapse.sql.azuresynapse.net:1433;database=chepra;user=chepra@cheprasynapse;password=KTM.ktm@12345;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;")
# MAGIC .option("tempDir", "wasbs://sampledata@chepra.blob.core.windows.net/temp")
# MAGIC .option("forwardSparkAzureStorageCredentials", "true")
# MAGIC .option("query", "select  * from dbo.trip")
# MAGIC .option("tempCompression","UNCOMPRESSED") //used to verify parq files in data lake
# MAGIC .load()
# MAGIC df.show()

# COMMAND ----------

