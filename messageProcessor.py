#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime

class MessageProcessor():
    global _table_body
    global _size
    log = 'DEBUG'
    def __init__(self, name_queue):
        _dt_now = datetime.datetime.now()
        self.queue_type = name_queue
        self._table_body=[]
    def receive(self, tag, body):
        _listener_date = datetime.datetime.now()
        # if self.log == 'DEBUG':
            # print("received at "+str(_listener_date))
        print(str(body))
        self._table_body.append(str(body))
        self._size=len(self._table_body)
        return self._table_body
        return self._size
        
    def get_table(self):
        return self._table_body
    def get_size(self):
        print(len(self._table_body))
        # self._size=len(self._table_body)
        return len(self._table_body)
        # return self._size