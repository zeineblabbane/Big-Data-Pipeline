from operator import delitem
import findspark
findspark.init()
from pyspark.sql import SparkSession
import random
from json import loads 
from kafka import KafkaConsumer
import os

def spark_stream():
    my_consumer = KafkaConsumer(  
        'projet',  
        bootstrap_servers = ['localhost : 9092'],  
        auto_offset_reset = 'earliest',  
        enable_auto_commit = True,  
        group_id = 'my-group',  
        value_deserializer = lambda x : loads(x.decode('utf-8'))  
        )  

    for message in my_consumer:  
        message = message.value  
        print(message)  
        f = open("football-after-kafka", "w")
        f.write(message["data"])
        f.close()
        break

    os.system("javac hbasefootball.java")
    os.system("java hbasefootball")

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.load("outputfootball/part-r-00000",format="csv", sep="\t")
    df.sort("_c1",ascending=False).show()


    def show2Teams(team1, team2):
        df.createOrReplaceTempView("footballscore")
        df2=spark.sql("SELECT * FROM footballscore WHERE _c0=='"+ team1+ "' OR _c0=='"+ team2 +"' ")
        
        score1=df2.collect()[0]['_c1']
        score2=df2.collect()[1]['_c1']

        if (score1<score2):
            return(team2)
        elif (score1>score2):
            return(team1)
        elif (score1==score2):
            return(df2.collect()[int(random.uniform(0, 1))]['_c0'])
        
    team1=input("Enter your first team: ")
    team2=input("Enter your second team: ")

    print(show2Teams(team1,team2))
