import pika
import os
# rmq_server = {'server': 'localhost', 'vhost': '/', 'user': 'guest', 'password': 'guest', 'port': 5672}
#rmq_server = {'server': 'vlg-ufm-mbus1b', 'vhost': 'ufmgf', 'user': 'ufm_gf', 'password': 'ufm_gf123', 'port': 5672}
#rmq_server = {'server': 'msk-ufm-mbus01', 'vhost': 'ufmgf', 'user': 'ufm_gf', 'password': 'ufm_gf', 'port': 5672}

rmq_server = {'server': 'msk-ufm-mbus05', 'vhost': 'ufmgf', 'user': 'ufm_gf', 'password': 'ufm_gf', 'port': 5672}
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

def catch_message(ch, method, properties, body):
    # msg_file.write(time.strftime('[%Y-%m-%d %H:%M:%S] '))
    msg_file.write('Method:\n')
    msg_file.write(str(method) + '\n')
    msg_file.write('Properties:\n')
    msg_file.write(str(properties) + '\n')
    msg_file.write('Body:\n')
    res_body = body.decode("utf-8")
    msg_file.write(res_body)
    msg_file.flush()


connection, channel = connect_to_rmq(rmq_server)
dirname = os.path.dirname(os.path.abspath(__file__))

msg_file = open(os.path.join(dirname, 'CLM-348459_2.txt'), 'w')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(catch_message, queue='trap.CLM-348459-canary_1', no_ack=True)
channel.start_consuming()

msg_file.close()
connection.close()