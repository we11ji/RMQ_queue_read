#!/usr/bin/env python
import pika
import os
import json
import sys



unique_client_messages = {}
republish = []
deleted_messages = []


#script parameters
rmq_server = {'server': 'localhost', 'vhost': '/', 'user': 'admin', 'password': 'admin', 'port': 5672}
queue_to_read = 'test'#'ps.ufm_delayed_check_q'
exchange_to_write = 'test' #'nx.ufm_b2x'
routing_key_to_write = 'test' #'ps.ufm_delayed_client_check'
total_messages = 10000


#
def connect_to_rmq(params):
    credentials = pika.PlainCredentials(params['user'], params['password'])
    parameters = pika.ConnectionParameters(
        host=params['server'],
        port=params['port'],
        virtual_host=params['vhost'],
        credentials=credentials,
        socket_timeout=5)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    print('connected to ' + params['server'])
    return connection, channel



def get_client(msg_body):
#find CLNT_ID value from msg_body (except dates)
#if CLNT_ID not found return None
	for key,value in msg_body.items():
		if key == "CLNT_ID":
			return value
		if isinstance(value, dict) and "Date" not in key:
			client = get_client(value)
			if client != None:
				return client
	return None

def callback(ch, method, properties, body, func):
#process each message with function in argument
	func(ch, method, properties, body)

def collect_unique(*args):
#collect unique clients in dictionary unique_client_messages
#dict structure : {client:[properties,[message]]}
#collect duplicates in dictionary deleted_messages

	for client_message in args[3]:
		client  = get_client(client_message)
		if client != None:
			if client not in unique_client_messages:
				#dict has structure {client: [properties,message]}
				unique_client_messages[client] = [args[2], [client_message]]
				print('Republishing client: {}'.format(client))
			else:
				deleted_messages.append([args[2], [client_message]])
		else:
			pass #no CLNT_ID found. should never be the case.

def process_one_client(*args):
#put first client to dictionary unique_client_messages
#delete all duplicates of this client
#if message is packed with several clients,:
#ack message and republish all clients different from first in unique_client_messages
	msg_packed = len(args[3]) > 1
	for client_message in args[3]:
		client  = get_client(client_message)
		if len(unique_client_messages) == 0:
			print('Remove duplicates of client: {}'.format(client))
			unique_client_messages[client] = [args[2], [client_message]]
		else:
			if client not in unique_client_messages:
				republish.append([args[2], [client_message]])
			else:
				deleted_messages.append([args[2], [client_message]])




def write_logs_to_file(filename,struct):
	dirname = os.path.dirname(os.path.abspath(__file__))
	file = open(os.path.join(dirname, filename), 'a+')
	if isinstance(struct, dict):
		for key,value in struct.items():
		    file.write('Client:'+key+'\n')
		    file.write('Messages:\n')
		    file.write(str(value)+'\n')
		    file.flush()
	if isinstance(struct, list):
		for item in struct:
			file.write(str(item[0]) + '\n')
			file.write(str(item[1]) + '\n')
			file.flush()
	file.close()

def publish(connection, channel, msg, props):
    exchange = exchange_to_write
    routing_key = routing_key_to_write
    channel.basic_publish(exchange, routing_key, msg, props)

def check_none(var):
	if var == None:
		return True

def main1(msg_amount):
#read messages from queue with auto acking
#collect unique clients and put first occurence of their message back to queueu
	connection, channel = connect_to_rmq(rmq_server)
	for x in range(msg_amount):
		method,properties,body = channel.basic_get(queue=queue_to_read,no_ack=True)
		#if queue is empty - break
		if check_none(method) == True:
			break
		#process message
		callback(channel,method,properties,json.loads(body.decode("utf-8")), collect_unique)
	#all handled messages write to log
	if len(unique_client_messages) > 0:
		write_logs_to_file('rewrited_log.txt',unique_client_messages)
		write_logs_to_file('deleted_log.txt',deleted_messages)
		#all unique messages write back to queue in the same order
		for key,value in unique_client_messages.items():
			publish(connection,channel,json.dumps(value[1]),value[0])
		print('Total clients republished: {}'.format(len(unique_client_messages)))
	else:
		print('Queue is empty')
	connection.close()

def main2(msg_amount):
#read first client
#ack all duplicate messages with this client
#republish first message to the end of queue
#nack packed messages with more than 1 message
	connection, channel = connect_to_rmq(rmq_server)
	for x in range(msg_amount):		
		method,properties,body = channel.basic_get(queue=queue_to_read,no_ack=True)
		#if queue is empty - break		
		if check_none(method) == True:
			break
		#process message
		callback(channel,method,properties,json.loads(body.decode("utf-8")), process_one_client)		
	#all handled messages write to log
	if len(unique_client_messages) > 0:
		write_logs_to_file('rewrited_log.txt',unique_client_messages)
		write_logs_to_file('deleted_log.txt',deleted_messages)
		#write first found 
		for item in republish:
			publish(connection,channel,json.dumps(item[1]),item[0])
		body = unique_client_messages[next(iter(unique_client_messages))][1]
		props = unique_client_messages[next(iter(unique_client_messages))][0]
		publish(connection,channel,json.dumps(body),props)
	else:
		print('Queue is empty')
	connection.close()


##################################################################

#if option passed then use it
#else read 10000 messages from rmq
if len(sys.argv) < 2:
	print('ERROR: wrong number of parameters. Execute example: "./python DeleteDuplicatesRMQ.py 1"')
else:
	if len(sys.argv) == 3 and int(sys.argv[2]) > 0:
		total_messages = int(sys.argv[2])
	if sys.argv[1] == '1':
		main1(total_messages)
	elif sys.argv[1] == '2':
		main2(total_messages)
	else: 
		print('ERROR: Function does not exist')