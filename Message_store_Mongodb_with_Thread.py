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
import time
from threading import Thread
def on_message_1(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor1.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
    # print("------------------------------------------------------------")
    # print(_processor1.get_size())
def on_message_2(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor2.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
    # print("------------------------------------------------------------")
    # print(_processor2.get_size())
def on_message_3(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor3.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
    print("------------------------------------------------------------")
    size_update=_processor3.get_size()
    print(size_update)
    size_update=0
  

def on_timeout():
  global connection
  connection.close() 
def thread_function_2():
    try:
        method_frame, header_frame, body =channel1.basic_get(queue=queue1)
        queue_size=method_frame.message_count
        print(queue_size)
        if(queue_size==0):
            channel1.stop_consuming()
    except:
        print("unable") 
    threading.Timer(10, thread_function_2).start()

def thread_function_1():
    _processor1 = messageProcessor.MessageProcessor('rav.data.spy1')
    credentials = pika.PlainCredentials('a686b90a-5134-4e00-ae44-0e3894e8bab9', 'm8t4i0kqm5kdnl2mqducbnda3b')
    vhost = '88f060bc-7e20-458b-b9ee-ff203501a0ba'
    queue1 = 'rav.data.SPY1'
    parameters = pika.ConnectionParameters(host='rmq-public.prod.eu.kamereon.org',
                                       port=5671,
                                       virtual_host=vhost,
                                       credentials=credentials,
                                       ssl=True,
                                       socket_timeout=300)

    # Create our connection object
    connection = BlockingConnection(parameters=parameters)

    print("Connection : " + str(connection.is_open))
    print()
    channel1 = connection.channel()
    message_properties = pika.BasicProperties(content_type="application/json")
    count=0

    try:
        channel1.start_consuming()
    except:
        print("unable to consume,either queue is empty or network prob")

def add_in_mongodb(tab_body,collection_name):
    dblist=[]
    myclient = MongoClient(host="localhost", port=27017)
    mydb = myclient["Dongle"]
    if (collection_name=="Status"):
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
    
    try:
        Thread.start( Thread_function_1 )
        Thread.start( Thread_function_2 )
    except Exception as e: print(e)

# while 1:
   # pass
    # while(count<queue_size):
        # count=count+1
        # print(count)
    # channel1.stop_consuming()
    # try:
        # channel1.start_consuming()
        # channel2.start_consuming()
        # channel3.start_consuming()

    # except KeyboardInterrupt:
        # print("Stopping")
        # channel1.stop_consuming()
        # channel2.stop_consuming()
        # channel3.stop_consuming()

       
    # tab_body_tracking=_processor1.get_table()
    # tab_body_status =_processor2.get_table()
    # tab_body_journey=_processor3.get_table()
        
    # add_in_mongodb(tab_body_tracking,"Tracking")
    # add_in_mongodb(tab_body_status,"Status")
    # add_in_mongodb(tab_body_journey,"Journey")


