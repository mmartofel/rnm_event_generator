from pika.adapters import SelectConnection, BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel

import pika
from pika.spec import BasicProperties

import messageProcessor

_processor = messageProcessor.MessageProcessor('Q_rav.data.SPY1')


def on_message(channel: BlockingChannel, method_frame, header_frame: BasicProperties, body):
    _processor.receive(method_frame.delivery_tag, body)
    channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)


credentials = pika.PlainCredentials('a686b90a-5134-4e00-ae44-0e3894e8bab9', 'm8t4i0kqm5kdnl2mqducbnda3b')
vhost = '88f060bc-7e20-458b-b9ee-ff203501a0ba'
queue = 'rav.data.SPY1'


parameters = pika.ConnectionParameters(host='rmq-public.prod.eu.kamereon.org',
                                       port=5671,
                                       virtual_host=vhost,
                                       credentials=credentials,
                                       ssl=True,
                                       socket_timeout=300)

#   Create our connection object
connection = BlockingConnection(parameters=parameters)

print("Connection : " + str(connection.is_open))
print()
channel = connection.channel()
message_properties = pika.BasicProperties(content_type="application/json")
channel.basic_consume(on_message, queue, no_ack=False, exclusive=False, consumer_tag='rav.data.SPY1')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Stopping")
    channel.stop_consuming()
connection.close()
print("Close")
