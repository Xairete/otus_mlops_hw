#!/usr/bin/env python
# coding: utf-8

import boto3
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import sum, avg, count, col, lit
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle

target_s3 = "s3a://mlops-german-hw3/"
with open('keys.pickle', 'rb') as handle:
    key_data = pickle.load(handle)

session = boto3.session.Session(aws_access_key_id = key_data['key'],                              aws_secret_access_key=key_data['secret'])

s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net')

filelist_s3 = [key['Key'] for key in s3.list_objects(Bucket='elenagerman-mlops-hw2')['Contents']]

spark = SparkSession.builder.getOrCreate()
schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("tx_datetime", TimestampType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", StringType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True)
    ])
raw_all = 0
processed_all = 0
for file_name in filelist_s3:
    try:
        print(file_name)
        df = spark.read.format("csv") \
                .option("header", "true") \
                .option("delimiter", ",") \
                .schema(schema) \
                .load(f"s3a://elenagerman-mlops-hw2/{file_name}")
        raw_count = df.count()
        print('Raw data count: ', raw_count)
        raw_all += raw_count
        
        df = df.dropDuplicates(['transaction_id'])
        df = df.filter(df["tx_datetime"].isNotNull())
        df = df.filter(~df["terminal_id"].cast(DoubleType()).isNull())
        
        processed_count = df.count()
        print('Processed data count: ', processed_count) 
        processed_all += processed_count
        df.write.parquet(f"{target_s3}{file_name.split('.')[0]}.parquet", mode="overwrite")
        
    except:
        print('Processing error: ', file_name)

print('Full raw data count: ', raw_all)
print('Full processed data count: ', processed_all)

