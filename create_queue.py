import pika
# реквизиты сервера и учетки
# rmq_server = {'server': 'localhost', 'vhost': '/', 'user': 'guest', 'password': 'guest', 'port': 5672}
rmq_server = {'server': 'msk-ufm-mbus05', 'vhost': 'ufmgf', 'user': 'ufm_gf', 'password': 'ufm_gf', 'port': 5672}
#rmq_server = {'server': 'msk-mbus-rmq06', 'vhost': '/', 'user': 'rabbit_support', 'password': 'H4ju3Zk!', 'port': 5672}
# подключение к серверу 
credentials = pika.PlainCredentials(rmq_server['user'], rmq_server['password'])
parameters = pika.ConnectionParameters(
    host=rmq_server['server'],
    port=rmq_server['port'],
    virtual_host=rmq_server['vhost'],
    credentials=credentials,
    socket_timeout=5)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='trap.ps.1_canary_ufm', passive = False, durable = True, exclusive = False, auto_delete = False, arguments = {})

channel.queue_bind(queue='trap.ps.1_canary_ufm',exchange='ps.1_canary_ufm_common', routing_key='ps.ufm_delete_subs_from_client')
channel.queue_bind(queue='trap.ps.1_canary_ufm',exchange='ps.1_canary_ufm_common', routing_key='ps.ufm_add_subs_to_client')

channel.queue_bind(queue='trap.ps.1_canary_ufm',exchange='ps.1_canary_ufm_common', routing_key='ps.ufm.clientinfo_lock')

#channel.queue_unbind(queue='trap.ps.1_canary_ufm',exchange='ps.ufm_common', routing_key='ps.ufm.client_lock')
#channel.queue_unbind(queue='trap.ps.1_canary_ufm',exchange='ps.ufm_common', routing_key='ps.ufm.clientinfo_lock')
#channel.queue_unbind(queue='trap.ps.1_canary_ufm',exchange='ps.ufm_common', routing_key='ps.ufm_add_subs_to_client')
#channel.queue_unbind(queue='trap.ps.1_canary_ufm',exchange='ps.ufm_common', routing_key='ps.ufm_delete_subs_from_client')


#channel.queue_unbind(queue='trap.CLM-348459',exchange='ps.chargesdb2ufm', routing_key='ps.chargesdb.subscriber_activity.*')
#channel.queue_delete( queue = 'trap.ps.1_canary_ufm', if_unused = True, if_empty = False)
connection.close()