from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from subprocess import Popen, DEVNULL
from kafka import KafkaConsumer, KafkaProducer
from sys import stdin, stdout
from kafkaTospark import *
import pymongo
import requests
import kafka
import time
import os


def mariaToMongo():
    
    spark = SparkSession.builder.appName("pyspark-mariadb").master("local[*]").getOrCreate()
    
    #Code below connects to MariaDB and gets the tables to Spark
    branch_temp =  spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/cdw_sapp",driver="com.mysql.cj.jdbc.Driver",dbtable="branch",user="root",password="root").load()
    creditcard_temp = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/cdw_sapp",driver="com.mysql.cj.jdbc.Driver",dbtable="creditcard",user="root",password="root").load()
    customer_temp = spark.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/cdw_sapp",driver="com.mysql.cj.jdbc.Driver",dbtable="customer",user="root",password="root").load()
      
    #Testing if the connections worked
    branch_temp.show(10)
    creditcard_temp.show(10)
    customer_temp.show(10)
      
    #Code below prints the table structure
    branch_temp.printSchema()
    creditcard_temp.printSchema()
    customer_temp.printSchema()
     
    #Code below creates a views for three tables
    branch_view = branch_temp.createOrReplaceTempView("branch_view")
    creditcard = creditcard_temp.createOrReplaceTempView("creditcard_view")
    customer = customer_temp.createOrReplaceTempView("customer_view")
     
    #Transformations based on views
    #Branch Transformation
    branch = spark.sql('SELECT BRANCH_CODE,BRANCH_NAME,BRANCH_STREET,BRANCH_CITY,BRANCH_STATE,ifnull(BRANCH_ZIP,99999) AS BRANCH_ZIP,concat(\'(\', SUBSTRING(BRANCH_PHONE,1,3),\')\', SUBSTRING(BRANCH_PHONE,4,3),\'-\', SUBSTRING(BRANCH_PHONE,7,4)) as BRANCH_PHONE, LAST_UPDATED FROM branch_view')
     
    #Creditcard Transformation
    creditcard = spark.sql('SELECT CREDIT_CARD_NO, cast(concat(YEAR, MONTH,DAY)as string) AS TIMEID,CUST_SSN AS SSN,BRANCH_CODE,TRANSACTION_TYPE,TRANSACTION_VALUE,TRANSACTION_ID FROM creditcard_view')
     
    #Customer Transformation
    customer = spark.sql('SELECT SSN,initcap(FIRST_NAME) as FIRST_NAME,lower(MIDDLE_NAME) as MIDDLE_NAME,\
                initcap(LAST_NAME) as LAST_NAME, CREDIT_CARD_NO,concat(APT_NO,\',\',STREET_NAME) as CUST_STREET,CUST_CITY,CUST_STATE,CUST_COUNTRY, \
                cast(CUST_ZIP as int), CONCAT(SUBSTRING(CUST_PHONE,1,3),\'-\', SUBSTRING(CUST_PHONE,4,7)) as CUST_PHONE, CUST_EMAIL, LAST_UPDATED FROM customer_view')
     
    #Transfer of data from spark to MongoDB
                
    uri = "mongodb://localhost/creditCard.dbs"
     
    spark_mongodb = SparkSession.builder.config("spark.mongodb.input.uri",uri).config("spark.mongodb.output.uri",uri).getOrCreate()
     
    branch.write.format("com.mongodb.spark.sql.DefaultSource").mode('overwrite').option('database','creditCard').option('collection','branch').save()
     
    creditcard.write.format("com.mongodb.spark.sql.DefaultSource").mode('overwrite').option('database','creditCard').option('collection','creditcard').save()
     
    customer.write.format("com.mongodb.spark.sql.DefaultSource").mode('overwrite').option('database','creditCard').option('collection','customer').save()
     
    print('\nData Transferred to Mongo!\n')


#PART 2 - URL to KAFKA
       
def URLtoKafka():
    #BenefitsCostSharing data tranfer to Kafka 
     
    url1 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partOne.txt'
    url2 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partTwo.txt'
    url3 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partThree.txt'
    url4 = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/BenefitsCostSharing_partFour.txt'
     
    total_records=0    
    #Initiate Kafka producer
    producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
     
    print('\nDownloading Dataset')
     
    #Get data from url1 to rdd
    
    response = requests.get(url1)
    #Trasfer data
    full_data = response.text
        
    #Remove Headers
    final_data = [line for line in full_data.splitlines()[1:]]
    total_records += len(final_data)
    print('\nTotal no of records-', len(final_data))
               
    #Send data to Kafka
    for record in final_data:
        producer.send('BenefitsCostSharing1',record.encode('utf-8'))
    producer.flush()

    
    #Get data from url2 to rdd
    time.sleep(10)
    response = requests.get(url2)
    #Transfer data
    full_data = response.text
        
    #Remove Headers
    final_data = [line for line in full_data.splitlines()[1:]]
    total_records += len(final_data)
    print('\nTotal no of records-', len(final_data))
               
    #Send data to Kafka
    for record in final_data:
        producer.send('BenefitsCostSharing2',record.encode('utf-8'))
    producer.flush()

    
    #Get data from url3 to rdd
    time.sleep(10)
    response = requests.get(url3)
    #Trasfer data
    full_data = response.text
        
    #Remove Headers
    final_data = [line for line in full_data.splitlines()[1:]]
    total_records += len(final_data)
    print('\nTotal no of records-', len(final_data))
               
    #Send data to Kafka
    for record in final_data:
        producer.send('BenefitsCostSharing3',record.encode('utf-8'))
    producer.flush()

    #Get data from url4 to rdd
    time.sleep(10)
    response = requests.get(url4)
    #Trasfer data
    full_data = response.text
        
    #Remove Headers
    final_data = [line for line in full_data.splitlines()[1:]]
    total_records += len(final_data)
    print('\nTotal no of records-', len(final_data))
               
    #Send data to Kafka
    for record in final_data:
        producer.send('BenefitsCostSharing4',record.encode('utf-8'))
    producer.flush()

    print('\nTotal no of BenefitsCostSharing records - ',total_records)
    time.sleep(10)
  
    #Network data transfer
     
    url = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/Network.csv'
     
    response = requests.get(url)
    full_data = response.text
     
    final_data = [line for line in full_data.splitlines()[1:]]
    print('\nTotal no of Network Records- ', len(final_data))
     
    for record in final_data:
        producer.send('Network',record.encode('utf-8'))
    producer.flush()
     
    #Service Area data transfer
     
    url = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/ServiceArea.csv'
     
    response = requests.get(url)
    full_data = response.text
     
    final_data = [line for line in full_data.splitlines()[1:]]
    print('\nTotal no of Service Area Records- ', len(final_data))
     
    for record in final_data:
        producer.send('ServiceArea',record.encode('utf-8'))
    producer.flush()
     
    #Insurance data transfer
     
    url = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/insurance.txt'
     
    response = requests.get(url)
    full_data = response.text
     
    final_data = [line for line in full_data.splitlines()[1:]]
    print('\nTotal no of Insurance Records- ', len(final_data))
     
    for record in final_data:
        producer.send('Insurance',record.encode('utf-8'))
    producer.flush()
     
    #Plan Attribute data transfer
     
    url = 'https://raw.githubusercontent.com/platformps/Healthcare-Insurance-Data/master/PlanAttributes.csv'
     
    response = requests.get(url)
    full_data = response.text
     
    final_data = [line for line in full_data.splitlines()[1:]]
    print('\nTotal no of Plan Attribute Records- ', len(final_data))
     
    for record in final_data:
        producer.send('Plan_Attribute',record.encode('utf-8'))
    producer.flush()
   
    time.sleep(15)
    print('\nStarting Kafka to Spark streaming!!\n')

                       