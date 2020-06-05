import datetime
import pika
import os
import time

# servers parameters
rmq_server = {'server': 'srv8-bezeq', 'vhost': 'ufm_8.16.0', 'user': 'admin', 'password': 'admin', 'port': 5672}


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


connection, channel = connect_to_rmq(rmq_server)


def task_27(connection, channel, n):
    exchange = 'nx.ufm_b2x'
    routing_key = 'ps.ufm_delayed_client_check'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {'application': 'UFM' , 'version': '1.0', 'eventDate': '20200427T115348.399'}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 correlation_id = '',
                                 delivery_mode=2,
                                 priority=0,
				 type='ps.ufm_delayed_client_check'
                                 )
    msg = '[{"messageDate":"1587998687079","objectType":"C","objectId":"9631442","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"9631442"},"actualDateMap":{"CLNT_ID":"1587998687079"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1587998687079","attemptNumber":0,"needToProcess":false},{"messageDate":"1587998687079","objectType":"C","objectId":"14606616'+str((n*2)%10)+'","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"14606616'+str((n*2)%10)+'"},"actualDateMap":{"CLNT_ID":"1587998687079"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1587998687079","attemptNumber":0,"needToProcess":false}]'

    channel.basic_publish(exchange, routing_key, msg, props)
    print(msg)
    


for x in range(200000):
	task_27(connection, channel, x%10)
#

connection.close()
