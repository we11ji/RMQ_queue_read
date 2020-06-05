import datetime
import pika
import os
import time

# servers parameters
rmq_server = {'server': 'srv8-jiugen.net.billing.ru', 'vhost': 'ufm_8.16.0', 'user': 'admin', 'password': 'admin', 'port': 5672}


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


def task_3(connection, channel):
    exchange = 'ps.ns_cart.charges'
    routing_key = '	ps.charge_period_payment_fail.#	'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.0', 'eventDate': '20180312T145241.827'},
                                 delivery_mode=2,
                                 priority=0,
                                 type='ps.serv_disable_delete'
                                 )

    msg = '[{"subscriberId":2341187,"productType":"services","productId":155,"subscriptionId":1,"subscriberProductId":"64152248","cost":0,"balance":-1010.3,"payDate":"2018-04-03T07:39:11+00:00","startDate":"2018-04-03T07:39:11+00:00","endDate":"2018-04-04T07:39:11+00:00","reportingStartDate":"2018-04-03T07:39:11+00:00","reportingEndDate":"2018-04-04T07:39:11+00:00","periodNumber":203,"productUserName":"SMS-чек","MSISDN":"9255137501","requestedVolume":1,"paidVolume":0,"paymentType":"RECURRING","reason":"BRT_SRV: Credit limit reached. balance=-1010.3; cost=0. internal:4012(CREDIT_LIMIT_REACHED)"}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_2(connection, channel):
    exchange = 'ps.ufm2cci'
    routing_key = 'ps.promised_payment_threshold_break_notify.#'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.0', 'eventDate': '20180312T145241.827'},
                                 delivery_mode=2,
                                 priority=0,
                                 type='ps.serv_disable_delete'
                                 )

    msg = '[{ "customerId": 1907,"subscriberId":7068,"MSISDN": "9257438119" }]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_4(connection, channel):
    exchange = 'ps.ns_cart.charges'
    routing_key = '	ps.charge_period_payment_fail.#	'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.0', 'eventDate': '20180312T145241.827'},
                                 delivery_mode=2,
                                 priority=0,
                                 type='ps.serv_disable_delete'
                                 )

    msg = '[{"subscriberId":2341187,"productType":"services","productId":155,"subscriptionId":1,"subscriberProductId":"64152248","cost":0,"balance":-1010.3,"payDate":"2018-04-03T07:39:11+00:00","startDate":"2018-04-03T07:39:11+00:00","endDate":"2018-04-04T07:39:11+00:00","reportingStartDate":"2018-04-03T07:39:11+00:00","reportingEndDate":"2018-04-04T07:39:11+00:00","periodNumber":203,"productUserName":"SMS-чек","MSISDN":"9255137501","requestedVolume":1,"paidVolume":0,"paymentType":"RECURRING","reason":"BRT_SRV: Credit limit reached. balance=-1010.3; cost=0. internal:4012(CREDIT_LIMIT_REACHED)"}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_5(connection, channel):
    exchange = 'ps.chargesdb2ufm'
    routing_key = 'ps.chargesdb.subscriber_activity.0'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.0', 'eventDate': '20190112T145241.827'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"from":"notifier-NA1-dm","activities":[{"subscriberId":77332315,"customerId":65408443,"eventDate":"2019-02-22T05:13:19","callId":"C80301841D201902I910003290795191864"}]}'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_6(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.service_activate.1'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.0', 'eventDate': '20190112T145241.827'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"subscriberId":121982300,"msisdn":"9271168600","orderId":null,"createUser":"CRAB","operationDate":"2019-08-23T09:51:51","customerId":112189400,"recurringFlag":null,"activationDate":"2018-11-21T05:02:09","subscriberZoneId":11,"serviceId":100,"serviceStatusId":4,"previousStatusId":1,"activationType":null,"marketingCampaign":{"marketingCampaignId":null,"deactivationDate":null,"endPeriodActionType":null,"type":null,"name":null},"MSISDN":"9271168600"}'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_7(connection, channel):
    exchange = 'ps.ufm_common'
    routing_key = 'ps.ufm_add_subs_to_client'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 content_encoding='UTF-8',
                                 correlation_id='',
                                 headers={'COUNT_ATTEMPT': 0, 'application': 'UFM',
                                          'LAST_ATTEMP_DATE': datetime.datetime(2019, 4, 10, 11, 41, 1),
                                          'FIRST_ATTEMPT_DATE': datetime.datetime(2019, 4, 10, 11, 41, 1),
                                          'version': '1.0',
                                          'NEXT_ATTEMPT_DATE': datetime.datetime(2019, 4, 10, 11, 41, 6),
                                          'eventDate': '20190410T144101.373'},
                                 delivery_mode=2,
                                 priority=0,
                                 type='ps.ufm_add_subs_to_client'
                                 )

    msg = '[{"messageDate":"1558796808406","objectType":"ASTC","objectId":"112189458","dataSourceId":11,"attributes":{"strMap":{"SUBS_ID":"121982364","CLNT_ID":"112189458"},"longMap":{"DATA_SOURCE_ID":11},"actualDateMap":{"SUBS_ID":"1558796808406","DATA_SOURCE_ID":"1558796808406","CLNT_ID":"1558796808406"},"removeDateMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"attemptNumber":0,"needToProcess":false}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_8(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.subs_real_time_status_update.11'
    dirname = os.path.dirname(os.path.abspath(__file__))

    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.0', 'eventDate': '20200111T145241.827'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '[{"customerId":11198019,"subscriberId":16103080,"realTimeStatusId":0}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_9(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.customer_credit_history_full_update.001'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2020-03-11T04:36:23',
                                          'MSG_ID': '3'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '[{"customerId":140765696,"operationDate":"2020-03-11T04:36:21","creditChangeReasonId":0,"warningAmount":0,"breakAmount":1,"activateAmount":1,"realTimeBreak":1,"bankId":null,"creditChangeTypeId":0,"comment":"176841495","endDate":"2999-12-31T00:00:00","eventTimeStamp":"2020-03-11T04:36:22.540+03:00","isNotBlocked":false}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_10(connection, channel):  # ps.kit_add создание клиента/абона
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.kit_add'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-05-29T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"customerId":11218931277,"customerInsertMethodId":4,"juralTypeId":1,"useMultipleBills":"true","customerType' \
          'Id":1,"customerCategoryId":1,"customerStatusId":2,"customerClassId":3,"customerPayTypeId":2,"associationId":null,' \
          '"accountNumber":"5130569677","macroRegionId":500,"branchId":105,"registrationCategoryId":3,"contracts":' \
          '[{"contractId":121785479,"contractNumber":"GF0103663905","contractClassId":1,"contractStatusId":1,"expireDate":null,"isMain":true}]' \
          ',"subscribers":[{"subscriberId":121982377,"standardId":1,"subscriberStatusId":2,"identification":"9271168689","activationDate":"2019-08-02T08:18:16+03:00",' \
          '"usePersonalBalanceDate":null,"currentZoneId":44,"ratePlanId":2291,"switchId":66,"services":[{"serviceId":99,"serviceStatusId":4,' \
          '"autoBreakId":1,"autoWarningId":1},{"serviceId":100,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},' \
          '{"serviceId":101,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":102,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},' \
          '{"serviceId":103,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":104,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},' \
          '{"serviceId":105,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":126,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},' \
          '{"serviceId":127,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":129,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},' \
          '{"serviceId":130,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1}],"packs":[],"products":[{"productId":15321857562,"productOfferingId":318986,' \
          '"productActionType":"ACTIVATE","activationDateTime":"2019-08-02T08:18:16+03:00","deactivationDateTime":null,"productServices":[],' \
          '"marketingCampaign":null,"compriseProducts":null},{"productId":15321857571,"productOfferingId":220,"productActionType"' \
          ':"ACTIVATE","activationDateTime":"2019-08-02T08:18:16+03:00","deactivationDateTime":null,"productServices":' \
          '[{"serviceId":2345924856,"serviceActionType":"ACTIVATE"}],"marketingCampaign":null,"compriseProducts":null},' \
          '{"productId":15321857572,"productOfferingId":250,"productActionType":"ACTIVATE","activationDateTime":"2019-08-02T08:18:16+03:00",' \
          '"deactivationDateTime":null,"productServices":[{"serviceId":2345924857,"serviceActionType":"ACTIVATE"}],"marketingCampaign":null,' \
          '"compriseProducts":null},{"productId":15321857573,"productOfferingId":251,"productActionType":"ACTIVATE","activationDateTime":"2019-08-02T08:18:16+03:00","deactivationDateTime":null,' \
          '"productServices":[{"serviceId":2345924858,"serviceActionType":"ACTIVATE"}],"marketingCampaign":null,"compriseProducts":null},{"productId":15321857574,"productOfferingId":252,' \
          '"productActionType":"ACTIVATE","activationDateTime":"2019-08-02T08:18:16+03:00","deactivationDateTime":null,"productServices":[{"serviceId":2345924859,"serviceActionType":"ACTIVATE"}],' \
          '"marketingCampaign":null,"compriseProducts":null}],"remove":null,"MSISDN":"9271168611"}]}'

    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_11(connection, channel):  # баланс
    exchange = 'ps.brt'
    routing_key = 'ps.brt.rt_balance'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2020-01-20T05:46:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

   # msg = '[{"event_time":"20120111T065650","clnt":112189413,"balance": -7023.98,"spent":0,"credit_limit":0}]'
    msg = '[{"event_time":"20200120T065650","clnt":52700295,"balance": -100000000050,"spent":0,"virtual_payments": 0,"fin_ac":0}]'

    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


#  112189400   subs 121982300
def task_12(connection, channel):
    exchange = 'ps.chargesdb2ufm'
    routing_key = 'ps.chargesdb.subscriber_activity'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-09-09T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"from":"bellman-NA","activities":[{"subscriberId":121982364,"customerId":112189458,"eventDate":"2019-09-10T09:58:19","callId":"C15220298D201909I950021415154831349"},{"subscriberId":121982364,"customerId":112189458,"eventDate":"2019-09-10T10:58:21","callId":"C6654446D201909I950021415154832564"}]}'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_13(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.service_change_parameters.1'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-09-09T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"customerId":112189400,"subscriberId":121982300,"changeDate":"2019-08-15T06:43:23+03:00","createUser":"SBMS:CRAB_APPL:CRAB","marketingCampaign":null,"oldMarketingCampaign":null,"subscriptionFeeParameters":null,"serviceId":113,"breakActionId":1,"breakNotificationId":1,"subscriberProductId":null,"productOfferingId":null}'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_14(connection, channel):
    exchange = 'ps.ns_cart.charges'
    routing_key = 'ps.charge_period_end'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-09-09T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '[{"subscriberId":132187526,"productType":"services","productId":151,"subscriptionId":4,"subscriberProductId":"13010259447","eventDate":"2019-06-07T01:01:10+00:00","operationDate":"2019-06-07T01:01:10+00:00"}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_15(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.rate_plan_change.1'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-09-09T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"subscriberId":121982315,"ratePlanId":2527,"operationDate":"2019-08-15T09:55:07","customerId":112189415,"orderId":3217911772,"recurringFlag":null,"activationDate":"2019-12-15T09:55:07","activationType":"NEW_SUBSCRIBER","productOfferingId":319807,"subscriberProductId":15411830079,"MSISDN":"9271168615"}'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


def task_17(connection, channel):
    exchange = 'ps.ufm2rules'
    routing_key = 'ps.ufm_client_check'
    date = int(time.time())
    date = str(date) + '000'
    dirname = os.path.dirname(os.path.abspath(__file__))
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2020-03-10T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '[{"messageDate":"' + date + '","objectType":"C","objectId":"140765696",' \
          '"dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"140765696"},"actualDateMap":{"CLNT_ID":"' + date + '"},' \
          '"removeDateMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"' + date + '","attemptNumber":0,"needToProcess":false}]'
    print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()


# [{"messageDate":"1570065879523","objectType":"C","objectId":"112189458","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"112189458"},"actualDateMap":{"CLNT_ID":"1570065879523"},"removeDateMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1570065879523","attemptNumber":0,"needToProcess":false}]

def task_18(connection, channel):
    exchange = 'ps.ufm_dms'
    routing_key = 'ps.rule_to_dms'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {"application": "UFM", "version": "1.0",
              "eventDate": datetime.datetime.now().strftime("%Y%m%dT%H%M%S.000")}
    props = pika.BasicProperties(content_type='application/json',
                                 content_encoding='UTF-8',
                                 expiration='50',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0,
                                 type='ps.rule_to_dms'
                                 )

    msg = '[{"messageDate":"' + str(date - 100) + '000","objectType":"C","objectId":"69519535","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"69519535"},"actualDateMap":{"CLNT_ID":"' + str(
            date - 100) + '000"},"removeDateMap":{}},"sanctionExecList":[],"originalRoutingKey":"ps.ufm_se_check","expirationTimeout":"50","processStartDate":"' + str(
            date - 100) + '000","childrenMessageInfo":[],"startDate":"' + str(
            date - 100) + '000","attemptNumber":0,"needToProcess":false,"ownerClientId":"11C112189400","routingMask":"ps.ufm_se_check"}]'
        #		print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()

def task_19(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.subscriber_customer_link_add.001'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {"application": "UFM", "version": "1.0",
              "eventDate": datetime.datetime.now().strftime("%Y%m%dT%H%M%S.000")}
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-09-20T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"customerId":112189400,"subscriberId":95055216,"MSISDN":"9244837017","subscriberStatusId":2,"services":[{"serviceId":99,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":129,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":127,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":130,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":100,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":101,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":102,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":103,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":104,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":105,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1},{"serviceId":126,"serviceStatusId":4,"autoBreakId":1,"autoWarningId":1}],"packs":[{"packId":3824,"traceNumber":13782504001}],"ratePlanId":2602,"recurringFlag":null,"operationDate":"2019-09-09T05:40:33+03:00","switchId":67}'
        #		print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()

def task_20(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.subscriber_add.001'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {"application": "UFM", "version": "1.0",
              "eventDate": datetime.datetime.now().strftime("%Y%m%dT%H%M%S.000")}
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2019-09-20T04:36:21'},
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '{"subscriberId":81182892,"operationDate":"2019-12-06T04:37:42","customerId":112189415,"standartId":1,"phoneNumber":"9246932307","ratePlanId":2528,"recurringFlag":null,"activationDate":"2019-12-20T04:36:21","subscriberStatusId":2,"switchId":52}'
        #		print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()
#
def task_21(connection, channel):
    exchange = 'ps.ufm_common'
    routing_key = 'ps.ufm.subscriberinfo_error'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {"application": "UFM", "version": "1.0",
              "COUNT_ATTEMPT": 1,
              "LAST_ATTEMP_DATE": 1572507856,
              "FIRST_ATTEMPT_DATE": 1572507856,
              "NEXT_ATTEMPT_DATE": 1572507857,
              "eventDate": datetime.datetime.now().strftime("%Y%m%dT%H%M%S.000")}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0
                                 )

    msg = '[{"messageDate":"1572507852979","objectType":"BS","objectId":"151","dataSourceId":11,"attributes":{"strMap":{"SUBS_ID":"132187526"},"longMap":{"SRLS_ID":151,"CHARGED":0},"bigDecMap":null,"dateMap":null,"actualDateMap":{"SRLS_ID":"1559869270000","CHARGED":"1559869270000","SUBS_ID":"1559869270000"},"listLongMap":null,"removeDateMap":{}},"sanctionExecList":[],"originalRoutingKey":"ps.charge_period_end","processStartDate":"1572507852979","childrenMessageInfo":[],"startDate":"1559869270000","attemptNumber":6,"lastAttemptDate":"1572507924000","firstAttemptDate":"1572507856000","nextAttemptDate":"1572507988000","messageErrorType":"ERROR_PUSHED","failedActorBeanName":"com.peterservice.ufm.service.BuildService","errorDescription":"CLIENT_ID_NOT_FOUND","needToProcess":false,"routingMask":"ps.charge_period_end"}]'
        #		print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)

    msg = '[{"messageDate":"1572507852935","objectType":"BS","objectId":"151","dataSourceId":11,"attributes":{"strMap":{"SUBS_ID":"132187526"},"longMap":{"SRLS_ID":151,"CHARGED":0},"bigDecMap":null,"dateMap":null,"actualDateMap":{"SRLS_ID":"1559869270000","CHARGED":"1559869270000","SUBS_ID":"1559869270000"},"listLongMap":null,"removeDateMap":{}},"sanctionExecList":[],"originalRoutingKey":"ps.charge_period_end","processStartDate":"1572507852935","childrenMessageInfo":[],"startDate":"1559869270000","attemptNumber":6,"lastAttemptDate":"1572507924000","firstAttemptDate":"1572507856000","nextAttemptDate":"1572507988000","messageErrorType":"ERROR_PUSHED","failedActorBeanName":"com.peterservice.ufm.service.BuildService","errorDescription":"CLIENT_ID_NOT_FOUND","needToProcess":false,"routingMask":"ps.charge_period_end"}]'
        #		print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    connection.close()
#

def task_23(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.subscriber_personal_customer_full_update.11'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {"application": "UFM", "version": "1.0",
              "eventDate": datetime.datetime.now().strftime("%Y%m%dT%H%M%S.000")}
    props = pika.BasicProperties(content_type='application/json',
                                 headers={'application': 'UFM', 'version': '1.2', 'eventDate': '2020-03-06T04:37:42'},
                                 delivery_mode=2,
                                 priority=0
                                 )
    msg = '{"customerId": 135634846 , "subscriberId": 140298530,"personalCustomerId": 104484002,"personalCustomerStatusId":1 ,"personalCustomerTypeId": 1,"operationDate": "2020-03-06T04:37:42"}'

    # msg = '{"subscriberId":81182892,"operationDate":"2019-12-06T04:37:42","customerId":112189415,"standartId":1,"phoneNumber":"9246932307","ratePlanId":2528,"recurringFlag":null,"activationDate":"2019-12-20T04:36:21","subscriberStatusId":2,"switchId":52}'
        #		print(msg)
    channel.basic_publish(exchange, routing_key, msg, props)
    print(msg)
    connection.close()

#

def task_22(connection, channel):
    exchange = 'nx.ufm_b2x'
    routing_key = 'ps.ufm_delayed_client_check'    
    date = int(time.time())
    Header = {"application": "UFM", "version": "1.0",
              "eventDate": datetime.datetime.now().strftime("%Y%m%dT%H%M%S.000")}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0,
			      	 type='ps.ufm_delayed_client_check'
                                 )
    msg = '[{"messageDate":"1584090061264","objectType":"C","objectId":"90933690","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"90933690"},"actualDateMap":{"CLNT_ID":"1584090061264"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1584090061264","attemptNumber":0,"needToProcess":false}]'

    channel.basic_publish(exchange, routing_key, msg, props)
    print(f'msg at {datetime.datetime.now()}')
    time.sleep(0.4)
    msg = '[{"messageDate":"1584090061364","objectType":"C","objectId":"90933690","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"90933690"},"actualDateMap":{"CLNT_ID":"1584090061364"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1584090061364","attemptNumber":0,"needToProcess":false}]'
    channel.basic_publish(exchange, routing_key, msg, props)
    print(f'msg at {datetime.datetime.now()}')
    time.sleep(0.4)
    msg = '[{"messageDate":"1584090061464","objectType":"C","objectId":"90933690","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"90933690"},"actualDateMap":{"CLNT_ID":"1584090061464"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1584090061464","attemptNumber":0,"needToProcess":false}]'
    channel.basic_publish(exchange, routing_key, msg, props)
    print(f'msg at {datetime.datetime.now()}')
    time.sleep(0.4)
    msg = '[{"messageDate":"1584090061564","objectType":"C","objectId":"90933690","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"90933690"},"actualDateMap":{"CLNT_ID":"1584090061564"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1584090061564","attemptNumber":0,"needToProcess":false}]'
    channel.basic_publish(exchange, routing_key, msg, props)
    print(f'msg at {datetime.datetime.now()}')
    time.sleep(0.4)
    msg = '[{"messageDate":"1584090061664","objectType":"C","objectId":"90933690","dataSourceId":11,"attributes":{"strMap":{"CLNT_ID":"90933690"},"actualDateMap":{"CLNT_ID":"1584090061664"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"childrenMessageInfo":[],"startDate":"1584090061664","attemptNumber":0,"needToProcess":false}]'
    channel.basic_publish(exchange, routing_key, msg, props)
    print(f'msg at {datetime.datetime.now()}')
    #print(msg)
    connection.close()

#

def task_24(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.subscriber_full_update'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {'MSG_ID': '22965934731', 'version': '1.0', 'eventDate': '2020-03-19T12:13:28'}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0,
				 type='ps.subscriber_full_update'
                                 )
    msg = '{"subscriberId":82958906,"operationDate":"2020-03-19T12:13:28","standartId":1,"subscriberStatusId":2,"identification":"","personalCustomerId":null,"activationDate":"2020-03-19T12:13:28","usePersonalBalanceDate":null,"currentZoneId":3}'
    msg = '{"customerId":70176159,"subscriberId":82958906,"operationDate":"2020-03-17T13:13:28","standartId":1,"subscriberStatusId":2,"identification":"","personalCustomerId":null,"activationDate":"2020-03-19T12:13:28","usePersonalBalanceDate":null,"currentZoneId":11}'
    channel.basic_publish(exchange, routing_key, msg, props)
    print(msg)
    connection.close()

#

def task_25(connection, channel):
    exchange = 'ps.msg_bis_notify'
    routing_key = 'ps.subscriber_add.001'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {'MSG_ID': '22965934736', 'version': '1.4', 'eventDate': '2020-03-17T13:13:28'}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0,
				 type='ps.subscriber_add.001'
                                 )
    msg = '{"subscriberId":82958906,"operationDate":"2020-03-18T13:13:28","customerId":70176159,"standartId":1,"phoneNumber":"9312909769","ratePlanId":455,"recurringFlag":null,"activationDate":"2020-03-19T13:13:28","subscriberStatusId":2}'
    channel.basic_publish(exchange, routing_key, msg, props)
    print(msg)
    connection.close()

#

def task_26(connection, channel):
    exchange = 'ps.ufm_common'
    routing_key = 'ps.ufm.error_rules'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {'MSG_ID': '22965934736','version': '1.4', 'eventDate': '20200423T115348.399','COUNT_ATTEMPT': 2, 'FIRST_ATTEMPT_DATE': 1587631369, 'LAST_ATTEMP_DATE': 1587632028, 'NEXT_ATTEMPT_DATE': 1587632668}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0,
				 type='ps.ufm.error_rules'
                                 )
    msg = '[{"messageDate":"1587631368440","objectType":"SDR","objectId":"10730201","dataSourceId":11,"attributes":{"strMap":{"SUBS_ID":"112845864","CLNT_ID":"96924242","CORRELATION_ID":"10730201","ERROR_TEXT":""},"listLongMap":{"SRLS_IDS":[150],"ACTION_IDS":[1]},"actualDateMap":{"SRLS_IDS":"1587631367000","ACTION_IDS":"1587631367000","SUBS_ID":"1587631367000","CLNT_ID":"1587631367000","CORRELATION_ID":"1587631367000","ERROR_TEXT":"1587631367000"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"originalRoutingKey":"ps.serv_disable_delete_result.1","processStartDate":"1587631368440","childrenMessageInfo":[],"startDate":"1587631367000","attemptNumber":1,"lastAttemptDate":"1587631707000","firstAttemptDate":"1587631369000","nextAttemptDate":"1587632027000","messageErrorType":"ERROR_PUSHED","failedActorBeanName":"com.peterservice.ufm.cachebusinesslayer.builder.client.common.ClientsBuilder","errorDescription":"CAN_NOT_UPDATE_BUSINESS_OBJECT","utilityAttributes":{"strMap":{"CLIENT_SEGMENT":"B2C"},"actualDateMap":{"CLIENT_SEGMENT":"1587631368444"},"removeDateMap":{},"attrsMap":{}},"needToProcess":false,"ownerClientId":"11C96924242","routingMask":"ps.separate_serv_disable_result"}]'

    channel.basic_publish(exchange, routing_key, msg, props)
    print(msg)
    connection.close()
#

def task_27(connection, channel):
    exchange = 'ps.ufm_common'
    routing_key = 'ps.ufm.error_rules'
    dirname = os.path.dirname(os.path.abspath(__file__))
    date = int(time.time())
    Header = {'MSG_ID': '22965934736','version': '1.4', 'eventDate': '20200423T115348.399','COUNT_ATTEMPT': 2, 'FIRST_ATTEMPT_DATE': 1587647808, 'LAST_ATTEMP_DATE': 1587650462, 'NEXT_ATTEMPT_DATE': 1587653022}
    props = pika.BasicProperties(content_type='application/json',
                                 headers=Header,
                                 delivery_mode=2,
                                 priority=0,
				 type='ps.ufm.error_rules'
                                 )
    msg = '[{"messageDate":"1587647799891","objectType":"SDR","objectId":"10730201","dataSourceId":11,"attributes":{"strMap":{"SUBS_ID":"47577","CLNT_ID":"35875","CORRELATION_ID":"10730201","ERROR_TEXT":""},"listLongMap":{"SRLS_IDS":[130],"ACTION_IDS":[0]},"actualDateMap":{"SRLS_IDS":"1587647799000","ACTION_IDS":"1587647799000","SUBS_ID":"1587647799000","CLNT_ID":"1587647799000","CORRELATION_ID":"1587647799000","ERROR_TEXT":"1587647799000"},"removeDateMap":{},"attrsMap":{}},"sanctionExecList":[],"originalRoutingKey":"ps.serv_disable_delete_result.1","processStartDate":"1587647799891","childrenMessageInfo":[],"startDate":"1587647799000","attemptNumber":1,"lastAttemptDate":"1587649168000","firstAttemptDate":"1587647808000","nextAttemptDate":"1587650448000","messageErrorType":"ERROR_PUSHED","failedActorBeanName":"com.peterservice.ufm.cachebusinesslayer.builder.client.common.ClientsBuilder","errorDescription":"CAN_NOT_UPDATE_BUSINESS_OBJECT","utilityAttributes":{"strMap":{"CLIENT_SEGMENT":"B2X"},"actualDateMap":{"CLIENT_SEGMENT":"1587647799905"},"removeDateMap":{},"attrsMap":{}},"needToProcess":false,"ownerClientId":"11C35875","routingMask":"ps.separate_serv_disable_result"}]'

    channel.basic_publish(exchange, routing_key, msg, props)
    print(msg)
    connection.close()


#
task_27(connection, channel)



