from kafka import KafkaConsumer
from json import loads
import credentials, json, urllib.request
import pyodbc

#-----------------Set up consumer
consumer = KafkaConsumer('tweets', group_id='tweets-batch', bootstrap_servers= 'PL-KAFKA-1')


#-----------set up connection to sql server
server = credentials.AzureSQLCredentials['server']
database = credentials.AzureSQLCredentials['database']
username = credentials.AzureSQLCredentials['username']
password = credentials.AzureSQLCredentials['password']
driver= credentials.AzureSQLCredentials['driver']


cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


for message in consumer:

    #----------------- receive message and load into json------------------
    msg = bytes.decode(message.value)
    msglist = msg.split(";")
    jsonMessage = json.loads(msg)
    print(jsonMessage)

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



    cursor.execute("insert into dbo.twitter_tweets(subject, created_at, tweet, source, user_name, user_screenname, user_location, user_followerscount, user_timezone,geo) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", subject, created_at, tweet, source, user_name, user_screenname, user_location, user_followerscount, user_timezone, geo)
    cnxn.commit()



cursor.close()
cnxn.close()  