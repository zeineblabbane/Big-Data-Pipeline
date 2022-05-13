from time import sleep  
from json import dumps 
import os
from kafka import KafkaProducer

def produce():
    os.system("hadoop fs -get outputfootball/part-r-00000")
    f = open("part-r-00000", "r")
    data=""
    for x in f:
        data = data + x

    # Kafka Producer
    my_producer = KafkaProducer(  
        bootstrap_servers = ['localhost:9092'],  
        value_serializer = lambda x:dumps(x).encode('utf-8')  
        )  

    for n in range(2):  
        my_data = {'data' : data}  
        my_producer.send('projet', value = my_data)  
        sleep(3)  
