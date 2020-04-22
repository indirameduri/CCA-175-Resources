from pyspark.sql import SparkSession, Row
from pyspark import SparkContext, SparkConf
from subprocess import Popen, DEVNULL
from kafka import KafkaConsumer, KafkaProducer
from sys import stdin, stdout
import pymongo
import requests
import kafka
import time
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
print('\nKafka->Spark-BenefitsCostSharing 1\n ')
#Topics transfer
spark = SparkSession.builder.master('local[*]').getOrCreate()
df1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing1").option("startingOffsets","earliest").load()
BenefitCS_df1 = df1.selectExpr("CAST(value AS STRING)")
output_query = BenefitCS_df1.writeStream.queryName("Benefits1").format("memory").start()
output_query.awaitTermination(10)
#output_query = BenefitCS_df1.writeStream.format("console").start()

BenefitCSdf1 = spark.sql('select * from Benefits1')
BenefitCSdf1.show(2)

BenefitCS_rdd1 = BenefitCSdf1.rdd.map(lambda i: i['value'].split('\t'))
BenefitCS_rdd1.sample(False,0.10)
Benefits_row_rdd1 = BenefitCS_rdd1.map(lambda i: Row(BenefitName = i[0],\
                                                   BusinessYear = i[1],\
                                                   EHBVarReason = i[2],\
                                                   IsCovered = i[3],\
                                                   IssuerId = i[4],\
                                                   LimitQty = i[5],\
                                                   LimitUnit = i[6],\
                                                   MinimumStay = i[7],\
                                                   PlanId = i[8],\
                                                   SourceName = i[9],\
                                                   StateCode = i[10]))
 
BDF1 = spark.createDataFrame(Benefits_row_rdd1)
BDF1.show(2)
#Spark to Mongo
uri = 'mongodb://localhost/healthInsurance.dbs'
spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
BDF1.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
print('\nData tranfered from Spark to Mongo!!')
spark.stop()
