# Databricks notebook source
import boto3

# COMMAND ----------

access_key = dbutils.secrets.get("lighthouse", "access_key")
secret_key = dbutils.secrets.get("lighthouse", "secret_key")

region = dbutils.widgets.get("region")
bucket_name = dbutils.widgets.get("bucket")
source_folder = dbutils.widgets.get("source_folder")
destination_folder = dbutils.widgets.get("destination_folder")

# COMMAND ----------

#Creating Session With Boto3.
session = boto3.Session(
aws_access_key_id=access_key,
aws_secret_access_key=secret_key,
region_name=region
)

#Creating S3 Resource From the Session.
s3 = session.resource('s3')

bucket = s3.Bucket(bucket_name)

for obj in bucket.objects.filter(Prefix="deployments"):
    source_key = obj.key
    destination_key = source_key.replace(source_folder, destination_folder, 1)
    s3.Object(bucket_name, destination_key).copy_from(CopySource={'Bucket': bucket_name, 'Key': source_key})
    s3.Object(bucket_name, source_key).delete()
