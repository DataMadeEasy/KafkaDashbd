from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['PL-KAFKA-1:9092'])


for e in range(1000):
    data = {'number' : e}
    producer.send('numtest', b'some_message_bytes')
    sleep(5)
    print(e)