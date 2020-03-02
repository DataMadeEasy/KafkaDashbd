from kafka import KafkaConsumer
from json import loads
import credentials, json, urllib.request
from urllib.error import HTTPError
import urllib3
import time
from datetime import datetime


#Get Endpoint
url = 'https://api.powerbi.com/beta/922c817f-0f15-4a6b-9095-5d20ce79b1f0/datasets/c745dc18-8ab9-4e7c-a437-2875f67e9b63/rows?noSignUpCheck=1&key=InEogAj8d2vSAj05WMKuoYNedlDDs24zObnmuYL1HWL3GONIGXpQJ%2BqL7V9CV0kSqvrh5etzweAum6%2BRuofhnw%3D%3D'

#------- Create Pool Manager
http = urllib3.PoolManager()


#-----------------Set up consumer
consumer = KafkaConsumer('numtest', group_id='tweet_stream', bootstrap_servers= 'PL-KAFKA-1')



for message in consumer:

    #----------------- receive message and load into json------------------
    msg = bytes.decode(message.value)
    msglist = msg.split(";")
    jsonMessage = json.loads(msg)
    #jsondatabytes = jsonMessage.encode('utf-8')

    #----- Parse Json---------------------
    subject = jsonMessage['subject']
    created_at = jsonMessage['created_at']
    tweet = jsonMessage["tweet"]
    source = jsonMessage["source"] 
    user_name = jsonMessage["user_name"]
    user_screenname = jsonMessage["user_screenname"]
    user_location = jsonMessage["user_location"]
    user_followerscount = jsonMessage["user_followerscount"]
    user_timezone = jsonMessage["user_timezone"]
    geo = jsonMessage["geo"]
    print(created_at)

    r = http.request('POST', url ,headers={'Content-Type': 'application/json'},body=msg)

    print (r.read()) 
    
