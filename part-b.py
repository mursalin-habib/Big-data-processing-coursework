import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()
    
    def good_line(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            str(fields[6])
            return True
        except:
            return False


    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    t_lines = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    c_lines = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv").rdd
    
    t_lines = t_lines.filter(lambda x: x[0] != "hash" )
    c_lines = c_lines.filter(lambda x: x[0] != "address" )
    
    t_clean_lines=t_lines.filter(good_line)
    
    features=t_clean_lines.map(lambda a: (a.split(',')[6],float(a.split(',')[7])))

    c_features = c_lines.map(lambda l: ( l[0],1))
    print(features.take(100))
    print(c_features.take(100))
    
    all_features=features.join(c_features)
    #result=all_features.map(lambda x:("{}-{}".format(x[1][0],x[1][1])))
    result=all_features.map(lambda x:(x[0],(float(x[1][0]),x[1][1])))
    print(all_features.take(100))
    
    result=result.reduceByKey(operator.add)
    
    top10=result.takeOrdered(10, key=lambda x: -x[1][0])
    
    for record in top10:
        print(" {};{}".format(record[0],record[1][0]))
    
                             
    now = datetime.now() 
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    my_result_object = my_bucket_resource.Object(s3_bucket,'ethereum_' + date_time + '/top10.txt')
    my_result_object.put(Body=json.dumps(top10))
    
    
    spark.stop()

