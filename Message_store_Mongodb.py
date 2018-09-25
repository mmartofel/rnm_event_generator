#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pika.adapters import SelectConnection, BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

import pika
import re , os , sys
import xlrd
import xlwt
from datetime import datetime
from pika.spec import BasicProperties
import time
import messageProcessor
from pymongo import MongoClient
import sys
from datetime import datetime
from pymongo import *
from pymongo.errors import ConnectionFailure
import json

_processor1 = messageProcessor.MessageProcessor('tracking')
_processor2 = messageProcessor.MessageProcessor('status')
_processor3 = messageProcessor.MessageProcessor('journey')
_processor4 = messageProcessor.MessageProcessor('rav.data.SPY1')

def on_message_1(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor1.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

def on_message_2(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor2.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
def on_message_3(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor3.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
def on_message_4(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor4.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
def kill(): 
    channel1.stop_consuming() 
    channel2.stop_consuming() 
    channel3.stop_consuming() 
    connection.close()
def add_in_mongodb(tab_body,collection_name):
    dblist=[]
    myclient = MongoClient(host="localhost", port=27017)
    mydb = myclient["Dongle"]
    # print(mydb.getCollection("Dongle"))
    print("*************************************************")
    if (collection_name=="Status"):
        # print("enter status collection")
        for i in range(0,len(tab_body)):
            dblist.append(json.loads(tab_body[i].replace("b'","").replace("}'","}"))) 
        mydb.Status.insert_many(dblist) 
        print("****************")
    if (collection_name=="Journey"):
        for i in range(0,len(tab_body)):
            dblist.append(json.loads(tab_body[i].replace("b'","").replace("}'","}"))) 
        mydb.Journey.insert_many(dblist) 
        print("++++++++++++++++")
    if (collection_name=="Tracking"):
        for i in range(0,len(tab_body)):
            dblist.append(json.loads(tab_body[i].replace("b'","").replace("}'","}"))) 
        mydb.Tracking.insert_many(dblist) 
        print("---------------")

    myclient.close()
if __name__ == "__main__":
    tab_body_tracking=[]
    tab_body_status=[]
    tab_body_journey=[]
    
    credentials = pika.PlainCredentials('kamereon.tsp.tsp10.pprd', 'NEm-F8z-d8p-F93')
    vhost = '88f060bc-7e20-458b-b9ee-ff203501a0ba'
    queue1 = 'acms.kamereon.tsp.TSP10.fleet.car.data.tracking.INSTANCE1'
    queue2 = 'acms.kamereon.tsp.TSP10.fleet.car.data.status.INSTANCE1'
    queue3 = 'acms.kamereon.tsp.TSP10.fleet.car.data.journey.INSTANCE1'


    parameters = pika.ConnectionParameters(host='rmq-public.prod.eu.kamereon.org',
                                           port=5671,
                                           virtual_host=vhost,
                                           credentials=credentials,
                                           ssl=True,
                                           socket_timeout=300)

    # Create our connection object
    connection = BlockingConnection(parameters=parameters)
    connection.add_timeout(10, kill) 

    print("Connection : " + str(connection.is_open))
    print()
    channel1 = connection.channel()
    channel2 = connection.channel()
    channel3 = connection.channel()
    message_properties = pika.BasicProperties(content_type="application/json")
    channel1.basic_consume(on_message_1, queue1, no_ack=False, exclusive=False, consumer_tag="TrackingReceiver")

    message_properties = pika.BasicProperties(content_type="application/json")
    channel2.basic_consume(on_message_2, queue2, no_ack=False, exclusive=False, consumer_tag="StatusReceiver")

    message_properties = pika.BasicProperties(content_type="application/json")
    channel3.basic_consume(on_message_3, queue3, no_ack=False, exclusive=False, consumer_tag="JourneyReceiver")

    try:
        channel1.start_consuming()
        channel2.start_consuming()
        channel3.start_consuming()
    except Exception as e: print(e)
    try: 
        tab_body_tracking=_processor1.get_table()
        tab_body_status  =_processor2.get_table()
        tab_body_journey =_processor3.get_table()
        if(len(tab_body_tracking)!=0):
            add_in_mongodb(tab_body_tracking,"Tracking")
        if(len(tab_body_status)!=0):
            add_in_mongodb(tab_body_status,"Status")
        if(len(tab_body_journey)!=0):
            add_in_mongodb(tab_body_journey,"Journey")
    except Exception as e: print(e)