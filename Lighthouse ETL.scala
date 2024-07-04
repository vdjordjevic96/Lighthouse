// Databricks notebook source
val sf_user = dbutils.secrets.get("etl-scope", "username")
val sf_password = dbutils.secrets.get("etl-scope", "password")
val sf_url = dbutils.secrets.get("etl-scope", "url")
val snowflake_db = dbutils.secrets.get("etl-scope", "sf_db_daily_etl")
val snowflake_schema = dbutils.secrets.get("etl-scope", "sf_schema_daily_etl")
val snowflake_wh = dbutils.secrets.get("etl-scope", "sf_wh_daily_etl")
val tableName = dbutils.widgets.get("tableName")
val bucket = dbutils.widgets.get("bucket")
val s3Folder = dbutils.widgets.get("s3Folder")

val options = Map(
  "sfUrl" -> sf_url,
  "sfUser" -> sf_user,
  "sfPassword" -> sf_password,
  "sfDatabase" -> snowflake_db,
   "sfSchema" -> snowflake_schema,
   "sfWarehouse" -> snowflake_wh
)

// COMMAND ----------

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(s"s3a://$bucket/$s3Folder")

df.show()

df.write.format("net.snowflake.spark.snowflake").options(options).option("dbtable", tableName).mode("append").save()
