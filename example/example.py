# encoding:utf-8
import datetime
import uuid

from tgasdk.sdk import TGAException, TGAnalytics, BatchConsumer, AsyncBatchConsumer, LoggingConsumer, DebugConsumer, \
    TGAIllegalDataException, \
    ROTATE_MODE

# BatchConsumer
# batchConsumer = BatchConsumer(server_uri="https://tga.thinkinggame.cn/", appid="1244e134b46480fa78ee6dfccbe8f3")
# #可选是否数据压缩方式，有gzip，none，内网传输可以传输False，默认True即gzip压缩
# batchConsumer = BatchConsumer(server_uri="url", appid="appid",
#                               compress=False)
# tga = TGAnalytics(batchConsumer)

# tga = TGAnalytics(AsyncBatchConsumer("url","appid"))

# 按照小时切分的 LoggingConsumer,默认按天切分
tga = TGAnalytics(LoggingConsumer(".", rotate_mode = ROTATE_MODE.HOURLY))

# tga =TGAnalytics(DebugConsumer("https://tga.thinkinggame.cn", "1244e134b46480fa78e6dfccbe8f3"))
# DebugConsumer不写入TA库,默认写入
#tga = TGAnalytics(DebugConsumer(server_uri="https://tga.thinkinggame.cn", appid="1244e1334b46480fa78ee6dfccbe8f3",write_data=True))

distinct_id = "ABD"
account_id = "11111"
# 用户属性
properties = {"OrderId": "abc_123",
              "lasttime": datetime.datetime.now(),
              "age": 44,
              "string1": "111"}
list = []
list.append('Google')
list.append('Runoob')
list.append('True')
list.append('2.222')
list.append('2020-02-11 14:17:43.471')
properties['arrkey4'] = list
try:
    tga.user_set(account_id=account_id, properties=properties)
except Exception as e:
    #异常处理
    raise TGAIllegalDataException(e)
tga.flush()

# 在一个用户的某一个或者多个集合类型
properties.clear()
properties = {'arrkey4': list, 'arrkey3': ['appendList', '222']}
try:
    tga.user_append(account_id=account_id, distinct_id=distinct_id, properties=properties)
except Exception as e:
    #异常处理
    raise TGAIllegalDataException(e)

# properties.clear()
# 删除某一个用户的属性
try:
    tga.user_unset(account_id, distinct_id, ["string1", "lasttime"])
except Exception as e:
    # 异常处理
    raise TGAIllegalDataException(e)

# 事件属性
properties.clear()
properties = {"OrderId": "abc_123",
              "#time": datetime.datetime.now(),
              "age": 33,
              "#uuid":uuid.uuid1(),#选填，必须是标准的uuid格式xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
              "#ip": "123.123.123.123"}
event_name = "zhouzhou"
try:
    tga.track(account_id=account_id, distinct_id=distinct_id, properties=properties, event_name=event_name)
except Exception as e:
    # 异常处理
    raise TGAIllegalDataException(e)

# 用户删除
# try:
#     tga.user_del(account_id=account_id, distinct_id=distinct_id)
# except Exception as e:
#     # 异常处理
#     raise TGAIllegalDataException(e)

tga.flush()
tga.close()
