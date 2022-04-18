

                                                        Comparing statistics of two different online currencies

import sys
import requests
from time import sleep
from confluent_kafka import Producer
from socket import gethostname

conf = {'bootstrap.servers': "sandbox-hdp:6667", 'client.id': gethostname()} 

kafka_topic = 'lab'

schema=StructType([StructField('code',StringType(),True),StructField('data',StructType([StructField('averagePrice',StringType(),True),StructField('buy',StringType(),True),StructField('changePrice',StringType(),True),StructField('changeRate',StringType(),True),StructField('high',StringType(),True),StructField('last',StringType(),True),StructField('low',StringType(),True),StructField('makerCoefficient',StringType(),True),StructField('makerFeeRate',StringType(),True),StructField('sell',StringType(),True),StructField('symbol',StringType(),True),StructField('takerCoefficient',StringType(),True),StructField('takerFeeRate',StringType(),True),StructField('time',LongType(),True),StructField('vol',StringType(),True),StructField('volValue',StringType(),True)]),True)])

PMP= []
producer = Producer(conf)

for Data in schema:
 response = requests.get("https://api.kucoin.com/api/v1/market/stats?symbol=BTC-USDT")
 PMP.append(response)
 producer.produce(kafka_topic, key="data", value=response.text)

for item in PMP:
 print( item.text )
