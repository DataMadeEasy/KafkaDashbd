
import credentials, json, urllib.request
from urllib.error import HTTPError
import urllib3
import time
from datetime import datetime

t = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(t)

#Create JSon Data
data = [{"Record": 1300, "Time":'2020-02-24T21:48:00.107Z'}]
jsondata = json.dumps(data)
jsondatabytes = jsondata.encode('utf-8')
print(jsondata)

#Get Endpoint
url = 'https://api.powerbi.com/beta/922c817f-0f15-4a6b-9095-5d20ce79b1f0/datasets/971ef8b9-f9b4-434f-8a08-caed9d9e3847/rows?key=mA3nlLvJAQ%2F97OYkPBSAl2wXUwBISRcSSXWvQX1Tkq2NcKbDWVvEbGpykjfKe05O22f3QS9rzuNUKTZXxpxXWg%3D%3D'


http = urllib3.PoolManager()

r = http.request('POST', url ,headers={'Content-Type': 'application/json'},body=jsondata)

print (r.read()) 