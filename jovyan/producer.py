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


def read_and_feed_file(conf,topic,filepath):
    producer = Producer(conf)
    with open(filepath,"r") as f:
        data = json.load(f)
    
    key = data['payload']['Account_Id__c']
    
    producer.produce(topic, data, key=key, callback=delivery_report,headers=json.dumps( {"src":"poc-script"}))
    try:
        producer.poll(0)
    except:
        traceback.print_exc()

    producer.flush()





if __name__ == "__main__":
    from cc_config_eu import conf
    read_and_feed_file(conf,sys.argv[1],sys.argv[2])
