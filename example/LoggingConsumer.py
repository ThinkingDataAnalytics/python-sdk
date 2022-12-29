import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from tgasdk.sdk import TGAException, TGAnalytics, BatchConsumer, AsyncBatchConsumer, LoggingConsumer, DebugConsumer, \
    TGAIllegalDataException, \
    ROTATE_MODE

tga = TGAnalytics(LoggingConsumer("./log", rotate_mode=ROTATE_MODE.HOURLY))

distinct_id = "ABD"
account_id = "11111"
try:
    tga.track(account_id=account_id, event_name='event_name', properties={'level': 0})
    tga.flush()
except Exception as e:
    print(e)
