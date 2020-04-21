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

#BenefitCostSharing 1

def BenefitsCostSharing1():
  
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
    print('\nKafka->Spark-BenefitsCostSharing 1\n ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing1").option("startingOffsets","earliest").load()
    BenefitCS_df1 = df1.selectExpr("CAST(value AS STRING)")
    output_query = BenefitCS_df1.writeStream.queryName("Benefits1").format("memory").start()
    output_query.awaitTermination(10)
    #output_query = BenefitCS_df1.writeStream.format("console").start()
    
    BenefitCSdf1 = spark.sql('select * from Benefits1')
    BenefitCSdf1.show(2)
    
    BenefitCS_rdd1 = BenefitCSdf1.rdd.map(lambda i: i['value'].split('\t'))
    
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


#BenefitCostSharing2 Topic
def BenefitsCostSharing2():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
    print('Kafka->Spark-BenefitsCostSharing 2 ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing2").option("startingOffsets","earliest").load()
    BenefitCS_df2 = df2.selectExpr("CAST(value AS STRING)")
    output_query = BenefitCS_df2.writeStream.queryName("Benefits2").format("memory").start()
    output_query.awaitTermination(25)
    #output_query = BenefitCS_df1.writeStream.format("console").start()
    
    BenefitCSdf2 = spark.sql('select * from Benefits2')
    
    
    BenefitCS_rdd2 = BenefitCSdf2.rdd.map(lambda i: i['value'].split('\t'))
    
    Benefits_row_rdd2 = BenefitCS_rdd2.map(lambda i: Row(BenefitName = i[0],\
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
     
    BDF2 = spark.createDataFrame(Benefits_row_rdd2)
    BDF2.show(2)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    BDF2.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop()
    
#BenefitsCostSharing 3

def BenefitsCostSharing3():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark-BenefitsCostSharing 3 ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df3 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing3").option("startingOffsets","earliest").load()
    BenefitCS_df3 = df3.selectExpr("CAST(value AS STRING)")
    print('BenefitsCostSharing3')


    output_query = BenefitCS_df3.writeStream.queryName("Benefits3").format("memory").start()
    output_query.awaitTermination(25)
    #output_query = BenefitCS_df1.writeStream.format("console").start()
    
    BenefitCSdf3 = spark.sql('select * from Benefits3')
    
    
    BenefitCS_rdd3 = BenefitCSdf3.rdd.map(lambda i: i['value'].split('\t'))
    
    Benefits_row_rdd3 = BenefitCS_rdd3.map(lambda i: Row(BenefitName = i[0],\
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
     
    BDF3 = spark.createDataFrame(Benefits_row_rdd3)
    BDF3.show(2)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    BDF3.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop()
    
def BenefitsCostSharing4():  
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark-BenefitsCostSharing 4 ')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    
    df4 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","BenefitsCostSharing4").option("startingOffsets","earliest").load()
    BenefitCS_df4 = df4.selectExpr("CAST(value AS STRING)")
    #BenefitsCostSharing 4
    output_query = BenefitCS_df4.writeStream.queryName("Benefits4").format("memory").start()
    output_query.awaitTermination(10)
    
    
    BenefitCSdf4 = spark.sql('select * from Benefits4')
    
    
    BenefitCS_rdd4 = BenefitCSdf4.rdd.map(lambda i: i['value'].split('\t'))
    
    Benefits_row_rdd4 = BenefitCS_rdd4.map(lambda i: Row(BenefitName = i[0],\
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
     
    BDF4 = spark.createDataFrame(Benefits_row_rdd4)
    BDF4.show(5)
    #Spark to Mongo
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    BDF4.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').option('database','healthInsurance').option('collection','BenefitsCostSharing').save()   
    print('\nData tranfered from Spark to Mongo!!')
    spark.stop()

#Network Topic
def Network():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'
    
    
    print('Kafka->Spark- Network')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","Network").option("startingOffsets","earliest").load()
    Network_df = df.selectExpr("CAST(value AS STRING)")
    print('\nNetwork Topic transfered to Spark')
    
    output_query = Network_df.writeStream.queryName("Network").format("memory").start()
    output_query.awaitTermination(10)
   
    
    Networkdf = spark.sql('select * from Network')
    
    
    Network_rdd = Networkdf.rdd.map(lambda i: i['value'].split(','))
    Network_row_rdd = Network_rdd.map(lambda i: Row(BusinessYear = i[0],\
                                                    StateCode = i[1],\
                                                    IssuerId = i[2],\
                                                    SourceName = i[3],\
                                                    VersionNum = i[4],\
                                                    ImportDate = i[5],\
                                                    IssuerId2 = i[6],\
                                                    StateCode2 = i[7],\
                                                    NetworkName = i[8],\
                                                    NetworkId = i[9],\
                                                    NetworkURL = i[10],
                                                    RowNumber = i[11],
                                                    MarketCoverage= i[12],
                                                    DentalOnlyPlan = i[13]))
    
    NDF = spark.createDataFrame(Network_row_rdd)
    NDF.show(5)
    
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    NDF.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','Network').save()
    print('Network transferred to MongoDB')
    spark.stop()
    
#ServiceArea Topic
def ServiceArea():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark- ServiceArea')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","ServiceArea").option("startingOffsets","earliest").load()
    ServiceA_df = df.selectExpr("CAST(value AS STRING)")
    
    output_query = ServiceA_df.writeStream.queryName("ServiceArea").format("memory").start()
    output_query.awaitTermination(10)
    
    
    ServiceA_df = spark.sql('select * from ServiceArea')
    
    
    ServiceA_rdd = ServiceA_df.rdd.map(lambda i: i['value'].split(','))
    ServiceA_row_rdd = ServiceA_rdd.map(lambda i: Row(BusinessYear = i[0],\
                                                      StateCode= i[1],\
                                                      IssuerId = i[2],\
                                                      SourceName = i[3],\
                                                      VersionNum = i[4],\
                                                      ImportDate = i[5],\
                                                      IssuerId2 = i[6],\
                                                      StateCode2 = i[7],\
                                                      ServiceAreaId = i[8],\
                                                      ServiceAreaName = i[9],\
                                                      CoverEntireState = i[10],\
                                                      County = i[11],\
                                                      PartialCounty = i[12],\
                                                      ZipCodes = i[13],\
                                                      PartialCountyJustification = i[14],\
                                                      RowNumber = i[15],\
                                                      MarketCoverage = i[16],\
                                                      DentalOnlyPlan = i[17]))
    
         
    SADF = spark.createDataFrame(ServiceA_row_rdd)
    SADF.show(5)
       
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    SADF.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','ServiceArea').save()
    print('\nServiceArea Topic transfered to Spark')
    spark.stop()
    
#Insurance Topic
def Insurance():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark- Insurance')
    #Topics transfer
    spark = SparkSession.builder.master('local[*]').getOrCreate()
    
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","Insurance").option("startingOffsets","earliest").load()
    Insurance_df = df.selectExpr("CAST(value AS STRING)")
    print('\nInsurance Topic transfered to Spark')
    
    output_query = Insurance_df.writeStream.queryName("Insurance").format("memory").start()
    output_query.awaitTermination(10)
    
    
    Insurance_df = spark.sql('select * from Insurance')
    
    Insurance_rdd = Insurance_df.rdd.map(lambda i: i['value'].split('\t'))
    Insurance_row_rdd = Insurance_rdd.map(lambda i: Row(age = i[0],\
                                                       sex = i[1],\
                                                       bmi = i[2],\
                                                       children = i[3],\
                                                       smoker = i[4],\
                                                       region = i[5],\
                                                       charges = i[6]))
    
         
    IDF = spark.createDataFrame(Insurance_row_rdd)
    IDF.show(5)
       
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    IDF.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','Insurance').save()
    print('Insurance Data transferred to MongoDB')
    spark.stop()
 
#Plan Attribute Topic
def Plan_Attribute():
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

    
    print('Kafka->Spark-Plan Attribute')
    #Topics transfer
    spark = SparkSession.builder.getOrCreate()
    
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","Plan_Attribute").option("startingOffsets","earliest").load()
    PlanA_df = df.selectExpr("CAST(value AS STRING)")
    print('\nPlanAttribute Topic Transfered to spark')
    
    output_query = PlanA_df.writeStream.queryName("PlanA").format("memory").start()
    output_query.awaitTermination(10)
    
    
    PlanA_df = spark.sql('select * from PlanA')
    
    PlanA_rdd = PlanA_df.rdd.map(lambda i: i['value'].split('\t'))
    PlanA_row_rdd = PlanA_rdd.map(lambda i: Row(AttributesID=i[0], \
                                               BeginPrimaryCareCostSharingAfterNumberOfVisits=i[1], \
                                               BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays=i[2], \
                                               BenefitPackageId=i[3], \
                                               BusinessYear=i[4], \
                                               ChildOnlyOffering=i[5], \
                                               CompositeRatingOffered=i[6], \
                                               CSRVariationType=i[7], \
                                               DentalOnlyPlan=i[8], \
                                               DiseaseManagementProgramsOffered=i[9], \
                                               FirstTierUtilization=i[10], \
                                               HSAOrHRAEmployerContribution=i[11], \
                                               HSAOrHRAEmployerContributionAmount=i[12], \
                                               InpatientCopaymentMaximumDays=i[13], \
                                               IsGuaranteedRate=i[14], \
                                               IsHSAEligible=i[15], \
                                               IsNewPlan=i[16], \
                                               IsNoticeRequiredForPregnancy=i[17], \
                                               IsReferralRequiredForSpecialist=i[18], \
                                               IssuerId=i[19], \
                                               MarketCoverage=i[20], \
                                               MedicalDrugDeductiblesIntegrated=i[21], \
                                               MedicalDrugMaximumOutofPocketIntegrated=i[22], \
                                               MetalLevel=i[23], \
                                               MultipleInNetworkTiers=i[24], \
                                               NationalNetwork=i[25], \
                                               NetworkId=i[26], \
                                               OutOfCountryCoverage=i[27], \
                                               OutOfServiceAreaCoverage=i[28], \
                                               PlanEffectiveDate=i[29], \
                                               PlanExpirationDate=i[30], \
                                               PlanId=i[31], \
                                               PlanLevelExclusions=i[32], \
                                               PlanMarketingName=i[33], \
                                               PlanType=i[34], \
                                               QHPNonQHPTypeId=i[35], \
                                               SecondTierUtilization=i[36], \
                                               ServiceAreaId=i[37], \
                                               sourcename=i[38], \
                                               SpecialtyDrugMaximumCoinsurance=i[39], \
                                               StandardComponentId=i[40], \
                                               StateCode=i[41], \
                                               WellnessProgramOffered=i[42]))
    
         
    PADF = spark.createDataFrame(PlanA_row_rdd)
    PADF.show(5)
       
    uri = 'mongodb://localhost/healthInsurance.dbs'
    spark_mongodb = SparkSession.builder.config('spark.mongodb.input.uri',uri).config('spark.mongodb.output.uri',uri).getOrCreate()
    PADF.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').option('database','healthInsurance').option('collection','PlanAttribute').save()
    print('Plan Attribute Data transferred to MongoDB')
    spark.stop()
