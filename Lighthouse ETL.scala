// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

val sf_user = dbutils.secrets.get("etl-scope", "username")
val sf_password = dbutils.secrets.get("etl-scope", "password")
val sf_url = dbutils.secrets.get("etl-scope", "url")
val snowflake_db = "FB_DB" //dbutils.secrets.get("etl-scope", "sf_db_daily_etl")
val snowflake_schema = "PERFORMANCE_DATA" //dbutils.secrets.get("etl-scope", "sf_schema_daily_etl")
val snowflake_wh = dbutils.secrets.get("etl-scope", "sf_wh_daily_etl")
val tableName = dbutils.widgets.get("tableName")
val bucket = dbutils.widgets.get("bucket")
val s3Folder = dbutils.widgets.get("s3Folder")
val schema = dbutils.widgets.get("schema")

val options = Map(
  "sfUrl" -> sf_url,
  "sfUser" -> sf_user,
  "sfPassword" -> sf_password,
  "sfDatabase" -> snowflake_db,
   "sfSchema" -> snowflake_schema,
   "sfWarehouse" -> snowflake_wh
)

val belgradeTimeZone = "Europe/Belgrade"

// COMMAND ----------

import org.apache.spark.sql.types._

val performanceSchema = StructType(Array(
  StructField("ID", StringType, true),
  StructField("TYPE", StringType, true),
  StructField("GATHERER", StringType, true),
  StructField("STATUS", StringType, true),
  StructField("LABEL", StringType, true),
  StructField("CREATEDTIMESTAMP", StringType, true),
  StructField("MODIFIEDTIMESTAMP", StringType, true),
  StructField("ERRORS", StringType, true),
  StructField("URL", StringType, true),
  StructField("psi.status", StringType, true),
  StructField("psi.statusText", StringType, true),
  StructField("psi.settings", StringType, true),
  StructField("psi.metadata.testId", StringType, true),
  StructField("psi.metadata.requestedUrl", StringType, true),
  StructField("psi.metadata.finalUrl", StringType, true),
  StructField("psi.metadata.lighthouseVersion", StringType, true),
  StructField("psi.metadata.userAgent", StringType, true),
  StructField("psi.metadata.fetchTime", StringType, true),
  StructField("psi.metrics.RenderBlockingResources", StringType, true),
  StructField("psi.metrics.crux.LargestContentfulPaint.category", StringType, true),
  StructField("psi.metrics.crux.LargestContentfulPaint.percentile", StringType, true),
  StructField("psi.metrics.crux.LargestContentfulPaint.good", StringType, true),
  StructField("psi.metrics.crux.LargestContentfulPaint.ni", StringType, true),
  StructField("psi.metrics.crux.LargestContentfulPaint.poor", StringType, true),
  StructField("psi.metrics.crux.FirstInputDelay.category", StringType, true),
  StructField("psi.metrics.crux.FirstInputDelay.percentile", StringType, true),
  StructField("psi.metrics.crux.FirstInputDelay.good", StringType, true),
  StructField("psi.metrics.crux.FirstInputDelay.ni", StringType, true),
  StructField("psi.metrics.crux.FirstInputDelay.poor", StringType, true),
  StructField("psi.metrics.crux.FirstContentfulPaint.category", StringType, true),
  StructField("psi.metrics.crux.FirstContentfulPaint.percentile", StringType, true),
  StructField("psi.metrics.crux.FirstContentfulPaint.good", StringType, true),
  StructField("psi.metrics.crux.FirstContentfulPaint.ni", StringType, true),
  StructField("psi.metrics.crux.FirstContentfulPaint.poor", StringType, true),
  StructField("psi.metrics.crux.CumulativeLayoutShift.category", StringType, true),
  StructField("psi.metrics.crux.CumulativeLayoutShift.percentile", StringType, true),
  StructField("psi.metrics.crux.CumulativeLayoutShift.good", StringType, true),
  StructField("psi.metrics.crux.CumulativeLayoutShift.ni", StringType, true),
  StructField("psi.metrics.crux.CumulativeLayoutShift.poor", StringType, true),
  StructField("psi.metrics.lighthouse.FirstContentfulPaint", StringType, true),
  StructField("psi.metrics.lighthouse.FirstMeaningfulPaint", StringType, true),
  StructField("psi.metrics.lighthouse.LargestContentfulPaint", StringType, true),
  StructField("psi.metrics.lighthouse.SpeedIndex", StringType, true),
  StructField("psi.metrics.lighthouse.TimeToInteractive", StringType, true),
  StructField("psi.metrics.lighthouse.FirstCPUIdle", StringType, true),
  StructField("psi.metrics.lighthouse.FirstInputDelay", StringType, true),
  StructField("psi.metrics.lighthouse.TotalBlockingTime", StringType, true),
  StructField("psi.metrics.lighthouse.CumulativeLayoutShift", StringType, true),
  StructField("psi.metrics.lighthouse.TotalSize", StringType, true),
  StructField("psi.metrics.lighthouse.HTML", StringType, true),
  StructField("psi.metrics.lighthouse.Javascript", StringType, true),
  StructField("psi.metrics.lighthouse.CSS", StringType, true),
  StructField("psi.metrics.lighthouse.Fonts", StringType, true),
  StructField("psi.metrics.lighthouse.Images", StringType, true),
  StructField("psi.metrics.lighthouse.Medias", StringType, true),
  StructField("psi.metrics.lighthouse.ThirdParty", StringType, true),
  StructField("psi.metrics.lighthouse.UnusedCSS", StringType, true),
  StructField("psi.metrics.lighthouse.WebPImages", StringType, true),
  StructField("psi.metrics.lighthouse.OptimizedImages", StringType, true),
  StructField("psi.metrics.lighthouse.ResponsiveImages", StringType, true),
  StructField("psi.metrics.lighthouse.OffscreenImages", StringType, true),
  StructField("psi.metrics.lighthouse.DOMElements", StringType, true),
  StructField("psi.metrics.lighthouse.Requests", StringType, true),
  StructField("psi.metrics.lighthouse.Performance", StringType, true),
  StructField("psi.metrics.lighthouse.Accessibility", StringType, true),
  StructField("psi.metrics.lighthouse.SEO", StringType, true),
  StructField("psi.metrics.lighthouse.BestPractices", StringType, true),
  StructField("psi.errors", StringType, true),
  StructField("errors.0", StringType, true),
  StructField("psi.errors.0", StringType, true)
))

// Define the schema for the DEPLOYMENTS table
val deploymentsSchema = StructType(Array(
  StructField("AUTHOR", StringType, true),
  StructField("COMMIT", StringType, true),
  StructField("SOURCE", StringType, true),
  StructField("TIMESTAMP", TimestampType, true)
))

val schemaFinal: StructType = schema match {
  case "performance" => performanceSchema
  case "deployments" => deploymentsSchema
  case _ => throw new IllegalArgumentException(s"Unknown schema type: $schema")
}


// COMMAND ----------

val df = spark.read
  .option("header", "true")
  .schema(schemaFinal)
  .csv(s"s3a://$bucket/$s3Folder")

val dfFinal = df
  .withColumn("CREATEDTIMESTAMP", $"CREATEDTIMESTAMP".cast("long"))
  .withColumn("CREATEDTIMESTAMP", from_unixtime($"CREATEDTIMESTAMP" / 1000).cast("timestamp"))
  .withColumn("CREATEDTIMESTAMP", from_utc_timestamp($"CREATEDTIMESTAMP", belgradeTimeZone))
  .withColumn("MODIFIEDTIMESTAMP", $"MODIFIEDTIMESTAMP".cast("long"))
  .withColumn("MODIFIEDTIMESTAMP", from_unixtime($"MODIFIEDTIMESTAMP" / 1000).cast("timestamp"))
  .withColumn("MODIFIEDTIMESTAMP", from_utc_timestamp($"MODIFIEDTIMESTAMP", belgradeTimeZone))

dfFinal.show()

dfFinal.write.format("net.snowflake.spark.snowflake").options(options).option("dbtable", tableName).mode("append").save()


