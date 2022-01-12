import json
import boto3
import random
import struct
import socket
import time
# References & snippet are taken from AWS documentation for Kinesis
from datetime import datetime
import secrets
import uuid

STREAM_NAME = "WebLogs"

def get_data():
    ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    now = datetime.utcnow()
    users = ['John','Jill','Matt','Randy','Kyla','Alex','Brandon','Nelson','Robert','Tom','Zendi',
            'Linda','Melissa','Sara','Peter','Riley','Lily']
    httpcode = ['200','400','304','505','202','500']
    browser = ['firefox','chrome','edge','safari']
    departments = ['apparel ','electronics/mobile','electronics/tv','electronics/tabs','electronics/watch'
                   ,'fashion','payment','checkout','home/decor','home/furniture','kitchen/utensils','kitchen/baking']
    clicktime = now.strftime("[%Y-%m-%d %H:%M:%S."+str(now.microsecond)+"]")
    # print(ip)
    return {'log': ip+' - '+users[random.randint(0,15)]+' '+clicktime+' '+browser[random.randint(0,3)]+
                   ' "GET /'+departments[random.randint(0,11)]+'/objectid='+str(uuid.uuid4())+' HTTP/1.1" '+httpcode[random.randint(0,5)]+' 0'}


def generate(stream_name, kinesis_client):
    while True:
        for i in range(0,random.randint(1,100)):
            data = get_data()
            #print(data)
            kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey="partitionkey")
        time.sleep(random.randint(1,5))


if __name__ == '__main__':
    generate(STREAM_NAME, boto3.client('kinesis',region_name='us-west-2'))
