
import credentials, json, urllib.request
from urllib.error import HTTPError
import urllib3

#Create JSon Data
data = {"username": 'tesuser', "tweet": 'testtweet',"count":15}
jsondata = json.dumps(data)
print(type(jsondata))
jsondatabytes = jsondata.encode('utf-8')

#Get Endpoint
url = credentials.PowerBICredentials['URLEndpoint']
print(url)

#Create Request 
#headers = {'content-type': 'application/json'}
#response = requests.post(url,data=jsondata, headers=headers)




http = urllib3.PoolManager()

r = http.request('POST', url ,headers={'Content-Type': 'application/json'},body=jsondata)

print (r.read()) 