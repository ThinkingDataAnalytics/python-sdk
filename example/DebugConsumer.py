import os
import sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from tgasdk.sdk import TGAException, TGAnalytics, BatchConsumer, AsyncBatchConsumer, LoggingConsumer, DebugConsumer, \
    TGAIllegalDataException, \
    ROTATE_MODE
tga =TGAnalytics(DebugConsumer("https://receiver-ta-demo.thinkingdata.cn",
                               "appId",
                               device_id="123456789"))
distinct_id = "ABD"
account_id = "11111"
try:
    tga.track(account_id=account_id, event_name='event_name', properties={'level':0})
except Exception as e:
    print(e)
