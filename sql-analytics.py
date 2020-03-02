from json import loads
import credentials, json
import pyodbc
import sched, time
import datetime
import urllib.request
from urllib.error import HTTPError
import urllib3




#--------------PowerBI Endpooint
url_main = credentials.PowerBICredentials['url_main']
url_biden = credentials.PowerBICredentials['url_biden']
url_bloomberg = credentials.PowerBICredentials['url_bloomberg']
url_buttigieg = credentials.PowerBICredentials['url_buttigieg']
url_sanders = credentials.PowerBICredentials['url_sanders']



http = urllib3.PoolManager()

#-----------set up connection to sql server
server = 'pl-dbserver-streamingdata.database.windows.net'
database = 'pl-db-streamingdata'
username = 'parcadmin'
password = 'ParcHyper10n'
driver= '{ODBC Driver 17 for SQL Server}'


cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()



s = sched.scheduler(time.time, time.sleep)

def run_tweet_analytics(sc):

    # Set Current Time and Previous Time 
    currentDT = datetime.datetime.now()
    currentDT = currentDT+datetime.timedelta(hours=5)
    currentDT = currentDT-datetime.timedelta(seconds=15)
    previousDT = currentDT- datetime.timedelta(seconds=45)


    #------------- Run for Subject 1 -------------------
    subject1 = 'Bloomberg'
    cursor.execute("select count(*) from dbo.twitter_tweets where subject = ? and created_at between ? and ?", (subject1,previousDT,currentDT))
    count1 = cursor.fetchone()[0]
    data1= [{"Subject":subject1, "Time":str(currentDT), "TweetCount":count1}]
    jsondata1 = json.dumps(data1)

    #----------send data to endpoint Subject 1
    r = http.request('POST', url_main ,headers={'Content-Type': 'application/json'},body=jsondata1)
    r = http.request('POST', url_bloomberg ,headers={'Content-Type': 'application/json'},body=jsondata1)


    #------------- Run for Subject 2 -------------------
    subject2 = 'Biden'    
    cursor.execute("select count(*) from dbo.twitter_tweets where subject = ? and created_at between ? and ?", (subject2,previousDT,currentDT))
    count2 = cursor.fetchone()[0]
    data2= [{"Subject":subject2, "Time":str(currentDT), "TweetCount":count2}]
    jsondata2 = json.dumps(data2)


    #----------send data to endpoint Subject 2
    r = http.request('POST', url_main ,headers={'Content-Type': 'application/json'},body=jsondata2)
    r = http.request('POST', url_biden,headers={'Content-Type': 'application/json'},body=jsondata2)

    #------------- Run for Subject  3-------------------
    subject3 = 'Buttigieg'    
    cursor.execute("select count(*) from dbo.twitter_tweets where subject = ? and created_at between ? and ?", (subject3,previousDT,currentDT))
    count3 = cursor.fetchone()[0]
    data3= [{"Subject":subject3, "Time":str(currentDT), "TweetCount":count3}]
    jsondata3 = json.dumps(data3)


    #----------send data to endpoint Subject 3
    r = http.request('POST', url_main ,headers={'Content-Type': 'application/json'},body=jsondata3)
    r = http.request('POST', url_buttigieg,headers={'Content-Type': 'application/json'},body=jsondata3)

    #------------- Run for Subject  4-------------------
    subject4 = 'Sanders'    
    cursor.execute("select count(*) from dbo.twitter_tweets where subject = ? and created_at between ? and ?", (subject4,previousDT,currentDT))
    count4 = cursor.fetchone()[0]
    data4= [{"Subject":subject4, "Time":str(currentDT), "TweetCount":count4}]
    jsondata4 = json.dumps(data4)


    #----------send data to endpoint Subject 4
    r = http.request('POST', url_main ,headers={'Content-Type': 'application/json'},body=jsondata4)
    r = http.request('POST', url_sanders,headers={'Content-Type': 'application/json'},body=jsondata4)



    # do your stuff
    s.enter(10, 1, run_tweet_analytics, (sc,))

s.enter(5, 1, run_tweet_analytics, (s,))
s.run()


cursor.close()
cnxn.close()  