from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import credentials
from time import sleep
import json
from kafka import KafkaProducer

#consumer key, consumer secret, access token, access secret.
ckey = credentials.TwitterCredentials['APIKey']
csecret = credentials.TwitterCredentials['APISecretKey']
atoken = credentials.TwitterCredentials['AccessToken']
asecret = credentials.TwitterCredentials['AccessTokenSecret']

producer = KafkaProducer(bootstrap_servers=['PL-KAFKA-1:9092'])



class listener(StreamListener):

    def on_data(self, data):
        #print(data)
        all_data = json.loads(data)
        tweet = all_data["text"]
        username = all_data["user"]["screen_name"]
        jsontest = {username:tweet}
        print(jsontest)
        jd = json.dumps(jsontest)
        producer.send('numtest', jd.encode('utf-8'))
        return(True)

    def on_error(self, status):
        print (status)

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["#Bloomberg"])
