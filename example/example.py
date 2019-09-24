#encoding:utf-8
import datetime
import threading
import time

from tgasdk import TGAException, TGAnalytics, BatchConsumer, AsyncBatchConsumer, LoggingConsumer, ROTATE_MODE

# tga = TGAnalytics(BatchConsumer("http://sdk.tga.thinkinggame.cn:9080/logagent","6524568489fd4e4780fbe61ce9333e1b"))
# tga = TGAnalytics(AsyncBatchConsumer("http://test:44444/logagent","quanjie-python-sdk"))

# 按照小时切分的 LoggingConsumer
tga = TGAnalytics(LoggingConsumer(".", rotate_mode = ROTATE_MODE.HOURLY))

distinct_id = "ABD"
account_id = "TA10010"
properties={"OrderId":"abc_123",
            "lasttime":datetime.datetime.now()}
event_name="zhouzhou"

tga.user_set(account_id = account_id, distinct_id = distinct_id, properties = properties)
tga.track(account_id = account_id, distinct_id = distinct_id, properties = properties, event_name=event_name)

tga.flush()
tga.close()

# properties = {
#     "#time":datetime.datetime.now() - datetime.timedelta(days=1),
#     "custome":datetime.datetime.now(),
#     # "#ip":"192.168.1.1",
#     "Product_Name":"a",
#     '#os':'windows',
#     "today":datetime.date.today(),
#     "nullkey":None,
#     "#safwue8382f83":"#测试"
#
# }
# i = 1
# tga.user_set('dis'+str(i),None)
# tga.track('dis'+str(i),None,"shopping",properties.copy())
#
# tga.flush()
#
# start = time.time()
# print(start)
# # for i in range(100000):
# #     tga.track('dis'+str(i),None,"shopping",properties)
# tga.close()
# end = time.time()
# print(end)
# print("diff:",end - start)







