#!/usr/bin/env python
import pika
import sys
import time

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
#channel.exchange_declare(exchange='topic_exchange_3', exchange_type='topic')

channel2 = connection.channel()

for x in range(500):
	message = 'x = ' + str(x)
	message2 = 'square x = ' + str(x*x)
	channel.basic_publish(exchange='topic_exchange', routing_key='test.test2', body=message)
	if x%3 == 0:
		channel.basic_publish(exchange='topic_exchange', routing_key='aaa', body='error***')
		print(" [x] Sent ***error***")
	print(" [x] Sent %r" % message)
	channel2.basic_publish(exchange='topic_exchange', routing_key='test.test', body=message2)
	print(" [x] Sent %r" % message2)
	time.sleep(0.4)

connection.close()