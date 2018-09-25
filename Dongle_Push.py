from pika.adapters import SelectConnection, BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

import pika
import sys
from pika.spec import BasicProperties
import sys
import uuid
import os
import time
# from datetime import datetime as datetime
def gen_data_to_push(itteration,name_file,systemid,communicationid,decalage):
    len_tab=[]
    tab_data=[]
    tab_update=[]
    tab_update_el3=[]
    table_timestamp=[]
    tab_header=[]
    file_read=open(name_file,"r")
    tab_line=file_read.readlines()
    len_line=len(tab_line)
    i=0
    j=0
    k=0
    l=0
    str_line=""
    str_line_new=""
    file_name_update="message_updated_"+str(systemid)+"_"+str(communicationid)+"_"+str(itteration)+".txt"
    file_new=open(file_name_update,"w")
    for i in range(0,len_line):
        if '{"system.id":' in tab_line[i]:
            str_data=str(tab_line[i])
            index = str_data.find('{"system.id":')
            tab_data.append(str_data[index:len(str_data)])
            tab_header.append(str_data[2:index])
    for j in range(0,len(tab_data)):#pour chaque ligne du fichier
        tab_update=tab_data[j].split(",")#les elements d'une ligne splité
        uid=str(uuid.uuid4().hex)
        tab_update[0]='"system.id":"'+systemid+'"'
        tab_update[1]='"communication.id":"'+communicationid+'"'
        tab_update[2]='"message.id":"'+uid+'"'
        tab_update_el3=tab_update[3].split(":{")
        timestapm_old=tab_update_el3[0].replace('"',"")
        timestapm_new=int(tab_update_el3[0].replace('"',""))+7200+int(decalage)
        timestapm_str=str(timestapm_new)
        tab_update[3]='"'+timestapm_str+'"'+":{"+tab_update_el3[1]
        str_line=tab_header[j]
        for k in range(0,len(tab_update)):
            str_line=str_line+','+str(tab_update[k])
        file_new.write(str_line.replace("'","").replace(',{"system','{"system').replace(':,"system.id":',':{"system.id":'))
    file_new.close()

if __name__ == "__main__":
    # the script will be called like this: python dongle_push.py systemid communicationid message_input.txt itteration path
    print(sys.argv[1])#systemid
    print(sys.argv[2])#communicationid
    print(sys.argv[3])#message_input.txt
    print(sys.argv[4])#itteration how many times the script will be executed for this vin
    print(sys.argv[5])#path
    os.chdir(sys.argv[5])
    
    systemid=sys.argv[1]
    communicationid=sys.argv[2]
    message_input=sys.argv[3]
    itteration=sys.argv[4]
    #for each systemid defined as parameter to the script
    for i in range(0,int(itteration)):
        gen_data_to_push(i,message_input,systemid,communicationid,3600*(i+1))

    
    credentials = pika.PlainCredentials('a686b90a-5134-4e00-ae44-0e3894e8bab9', 'm8t4i0kqm5kdnl2mqducbnda3b')
    vhost = '88f060bc-7e20-458b-b9ee-ff203501a0ba'
    queue = 'rav.data.amq'


    parameters = pika.ConnectionParameters(host='rmq-public.prod.eu.kamereon.org',
                                           port=5671,
                                           virtual_host=vhost,
                                           credentials=credentials,
                                           ssl=True,
                                           socket_timeout=300)

    connection = BlockingConnection(parameters=parameters)

    print("Connection : " + str(connection.is_open))
    print()
    channel = connection.channel()
    try:
        for file in os.listdir(os.curdir):
            mspro = file.endswith('.txt')
            if(("updated" in file)and(mspro==True)):
                file_=open(file,'r')
                for body in file_.readlines():
                
                    if channel.basic_publish(exchange='rav.data',
                                        routing_key='',
                                        body= body.replace('\n',""),
                                        properties=pika.BasicProperties(content_type="application/json",delivery_mode=1)):
                                            print("OK push data on queue")
                print("file:",file,":OK")          
                file_.close()
                # os.remove(file)
    except KeyboardInterrupt:
        print("Stopping")
        channel.stop_consuming()
    connection.close()
    print("Close")
    filenames_new = os.listdir(os.curdir)
