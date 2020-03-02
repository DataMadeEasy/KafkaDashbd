from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import credentials
from time import sleep
import json
from kafka import KafkaProducer
import time



def stream_twitter_topic(param_subject):

    #consumer key, consumer secret, access token, access secret.
    ckey = credentials.TwitterCredentials['APIKey']
    csecret = credentials.TwitterCredentials['APISecretKey']
    atoken = credentials.TwitterCredentials['AccessToken']
    asecret = credentials.TwitterCredentials['AccessTokenSecret']

    #----------------Create a KafkaProducer
    producer = KafkaProducer(bootstrap_servers=['PL-KAFKA-1:9092'])



    #-----------------Pick a topic you would like to follow ----------------
    subject = param_subject




    class listener(StreamListener):

        def on_data(self, data):
            #print(data)
            all_data = json.loads(data)
            #print (all_data)
            #-------------Break tweet down in fields------------
            created_at = all_data["created_at"]
            created_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.strptime(created_at,'%a %b %d %H:%M:%S +0000 %Y'))
            tweet = all_data["text"]
            source = all_data["source"] 
            user_name = all_data["user"]["name"]
            user_screenname = all_data["user"]["screen_name"]
            user_location = all_data["user"]["location"]
            user_followerscount = all_data["user"]["followers_count"]
            user_timezone = all_data["user"]["time_zone"]
            geo = all_data["geo"]   
        


            #----------Create JSON object with data you care about -------------
            jsontest = {"subject":subject, "created_at": created_at, "tweet": tweet, "source": source,  "user_name": user_name, "user_screenname": user_screenname, "user_location": user_location, "user_followerscount": user_followerscount, "user_timezone": user_timezone, "geo": geo}
            print(jsontest)


            #-----------Send data to Kafka---------------------
            jd = json.dumps(jsontest)
            producer.send('tweets', jd.encode('utf-8'))
            return(True)

        def on_error(self, status):
            print (status)

    #-----------Create authorization for twitter
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)


    #----------Create twitter stream
    twitterStream = Stream(auth, listener())
    twitterStream.filter(track=[subject])
