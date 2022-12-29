# encoding:utf-8
import datetime
import uuid

import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from tgasdk.sdk import TGAException, TGAnalytics, BatchConsumer, AsyncBatchConsumer, LoggingConsumer, DebugConsumer, \
    TGAIllegalDataException, \
    ROTATE_MODE

# BatchConsumer
# batchConsumer = BatchConsumer(server_uri="http://localhost:port/", appid="APPID")
# batchConsumer = BatchConsumer(server_uri="url", appid="appid",
#                               compress=False)
# tga = TGAnalytics(batchConsumer)
# tga = TGAnalytics(AsyncBatchConsumer("url","appid"))

tga = TGAnalytics(LoggingConsumer("./log", rotate_mode=ROTATE_MODE.HOURLY))

# tga =TGAnalytics(DebugConsumer("https://receiver-ta-demo.thinkingdata.cn",
#                                "appid",
#                                device_id="123456789"))
# DebugConsumer
# tga = TGAnalytics(DebugConsumer(server_uri="http://localhost:port", appid="APPID",write_data=True))

distinct_id = "ABD"
account_id = "11111"

# properties = {"OrderId": "abc_123",
#               "lasttime": datetime.date.today(),
#               "age": 44,
#               "string1": "111",
#               "#time": datetime.datetime.now().replace(second=0)}
# list = []
# list.append('Google')
# list.append('Runoob')
# list.append('True')
# list.append('2.222')
# list.append('2020-02-11 14:17:43.471')
# properties['arrkey4'] = list

# dict = {'Name': 'Zara', 'Age': 7, 'Class': 'First', 'Time': datetime.datetime.now(),
#         'exam': {'Time': 'aaaaa'}}

# properties['dict'] = dict


# try:
#     tga.user_set(account_id=account_id, properties=properties)
# except Exception as e:
#     raise TGAIllegalDataException(e)
# tga.flush()

# properties.clear()
# properties = {'arrkey4': list, 'arrkey3': ['appendList', '222'], 'dict1': {'name': 'Tom', 'Age': 28}}
# try:
#     tga.user_append(account_id=account_id, distinct_id=distinct_id, properties=properties)
# except Exception as e:
#     raise TGAIllegalDataException(e)

# # properties.clear()

# try:
#     tga.user_unset(account_id, distinct_id, ["string1", "lasttime"])
# except Exception as e:

#     raise TGAIllegalDataException(e)

# properties.clear()
# properties = {"OrderId": "abc_123",
#               "#time": datetime.date.today(),
#               "age": 33,
#               "#uuid": uuid.uuid1(),  #optional，format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
#               "#ip": "123.123.123.123"}
# event_name = "zhouzhou"

# json = {"a": "a", "b": "b"}
# json_array = [json, {"c": "c"}]
# properties['json'] = json
# properties['json_array'] = json_array
# try:
#     tga.track(account_id=account_id, distinct_id=distinct_id, properties=properties, event_name=event_name)
# except Exception as e:
#     raise TGAIllegalDataException(e)


# try:
#     tga.user_del(account_id=account_id, distinct_id=distinct_id)
# except Exception as e:
#     raise TGAIllegalDataException(e)


# properties.clear()
# properties = {"tkey":datetime.datetime.now(),
#               "tkey2":datetime.date.today(),
#               'tkey3':[datetime.datetime.now(),datetime.date.today(),{"tkey2":datetime.date.today()}],
#               'tkey4':{"tkey2":datetime.date.today(),"tkey":datetime.datetime.now()},
#               "#time": datetime.datetime.now()}
# event_name = "eventName"
# try:
#     tga.track(account_id=account_id, distinct_id=distinct_id, properties=properties, event_name=event_name)
# except Exception as e:
#     raise TGAIllegalDataException(e)


# class DynamicPropertiesTracker(DynamicSuperPropertiesTracker):
#     def get_dynamic_super_properties(self):
#         return {'test_dynamic_key':'test_dynamic_value'}

# tga.set_dynamic_super_properties_tracker(DynamicPropertiesTracker())


# try:
#     tga.track_first(account_id=account_id, distinct_id=distinct_id, event_name='first_event', first_check_id='9999s0dadad', properties=properties)
# except Exception as e:
#     raise TGAIllegalDataException(e)
    
# #user_uniq_append
# properties.clear()
# properties = {'arrkey4': ['addValue','True'], 'arrkey3': ['appendList', '222']}
# try:
#     tga.user_uniq_append(account_id=account_id, distinct_id=distinct_id, properties=properties)
# except Exception as e:
#     raise TGAIllegalDataException(e)

# tga.flush()



# properties = {"#ip":'12.12.12.12',"#first_check_id":'123',"#app_id":'12',"#time":datetime.datetime.now(),'#uuid':'DDDDDSA'}

event_name = "eventName"
try:
    tga.track(account_id=account_id, distinct_id=distinct_id, properties={}, event_name=event_name)
except Exception as e:
    print(e)
    # raise TGAIllegalDataException(e)
tga.flush()
tga.close()
