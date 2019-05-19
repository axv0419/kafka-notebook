import datetime
import time
import sys
import yaml
import json

from confluent_kafka import Producer,Consumer,KafkaError
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from collections import defaultdict
import argparse
import traceback



def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered {} {} {} [{}]'.format( msg.timestamp(),msg.offset(), msg.topic(), msg.partition()))


seqMap = defaultdict(int)
def read_and_feed(conf,filelike,sep='|'):
    producer = Producer(conf)
    def parse_line_and_send(line):
        topic,key,data = line.split(sep)
        data = json.loads(data)
        skey = topic+key
        seqMap[skey]+= 1
        data['seqNo'] = seqMap[skey]
        data['createdTime'] = int(time.time() *1000)
        data = json.dumps(data)
        # time.sleep(.01)
        producer.produce(topic, data, key=key, callback=delivery_report,headers={"src":"jupiter"})
        try:
            producer.poll(0)
        except:
            traceback.print_exc()

    for line in filelike:
        line = line.strip()
        if line in ['q','exit','quit'] : break
        try:
#             print(f"line {line}")
            if line and line[0] != '#':
                parse_line_and_send(line)
        except:
            traceback.print_exc()
    producer.flush()


def read_and_feed_avro(conf,filelike,key_schema_str,value_schema_str,sep='|'):

    value_schema = avro.loads(value_schema_str)
    key_schema = avro.loads(key_schema_str)

    avroProducer = AvroProducer(conf, default_key_schema=key_schema, default_value_schema=value_schema)

    def parse_line_and_send(line):
        topic,key,data = line.split(sep)
        data = json.loads(data)
        skey = topic+key
        # data = json.dumps(data)
        avroProducer.produce(topic=topic, value=data, key=key, callback=delivery_report,headers={"src":"jupiter.avro"})
        try:
            avroProducer.poll(0)
        except:
            traceback.print_exc()

    for line in filelike:
        line = line.strip()
        if line in ['q','exit','quit'] : break
        try:
#             print(f"line {line}")
            if line and line[0] != '#':
                parse_line_and_send(line)
        except:
            traceback.print_exc()
    avroProducer.flush()


def consume_few(conf,kafka_topic,count):
    groupId = 'jupyter'
    _conf = dict(conf)
    _conf['group.id'] = groupId

    c = Consumer(_conf)
    # c.subscribe(['test-topic'])
    c.subscribe([kafka_topic])
    while count > 0:
        msg = c.poll(1.0)
        count -= 1
        if msg is None:
            break
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        # print('Received message: {}'.format(msg.value().decode('utf-8')))
        print(msg.topic(), msg.headers(),msg.value())
        # print('Received message: {}'.format(msg))
    c.close()