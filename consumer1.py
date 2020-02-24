from kafka import KafkaConsumer
from json import loads
import credentials, json, urllib.request
from urllib.error import HTTPError
import urllib3


consumer = KafkaConsumer('numtest', group_id='my-group', bootstrap_servers= 'PL-KAFKA-1')
url = credentials.PowerBICredentials['URLEndpoint']
http = urllib3.PoolManager()

for message in consumer:
    msg = bytes.decode(message.value)
    msglist = msg.split(";")
    d = json.loads(msg)
    print(type(d))
    print(d)
    r = http.request('POST', url ,headers={'Content-Type': 'application/json'},body=msg)