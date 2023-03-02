# encoding:utf-8
import sys
from tgasdk.sdk import *

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

TGAnalytics.enableLog(isPrint=False)

# consumer = DebugConsumer(server_uri="http://localhost:port/", appid="APPID", device_id="123456789")
# consumer = BatchConsumer(server_uri="http://localhost:port/", appid="APPID")
# consumer = AsyncBatchConsumer(server_uri="http://localhost:port/", appid="APPID")
consumer = LoggingConsumer("./log", rotate_mode=ROTATE_MODE.HOURLY, buffer_size=1)

te = TGAnalytics(consumer, strict=False)

distinct_id = "ABD"
account_id = "11111"

try:
    # init property: 'name', 'count', 'arr'
    user_set_properties = {'name': 'test', 'count': 1, 'arr': ['111', '222']}
    te.user_set(account_id=account_id, distinct_id=distinct_id, properties=user_set_properties)
except Exception as e:
    raise TGAIllegalDataException(e)

try:
    # Can only be set once
    user_set_once_properties = {'set_once_mac': "111bbb"}
    te.user_setOnce(account_id, distinct_id, user_set_once_properties)
except Exception as e:
    raise TGAIllegalDataException(e)

try:
    # clean property: 'name' now is nil
    user_unset_properties = ['name']
    te.user_unset(account_id, distinct_id, user_unset_properties)
except Exception as e:
    raise TGAIllegalDataException(e)

try:
    # append property: 'arr' now is ['111', '222', '222', '333']
    user_append_properties = {'arr': ['222', '333']}
    te.user_append(account_id=account_id, distinct_id=distinct_id, properties=user_append_properties)
except Exception as e:
    raise TGAIllegalDataException(e)

try:
    # unique append property: 'arr' now is ['111', '222', '333']
    user_unique_properties = {'arr': ['222', '333']}
    te.user_uniq_append(account_id=account_id, distinct_id=distinct_id, properties=user_unique_properties)
except Exception as e:
    raise TGAIllegalDataException(e)

try:
    # in previous 'user_set()', 'count' is 1. it will be 5 after 'user_add()'
    user_add_properties = {"count": 4}
    te.user_add(account_id, distinct_id, user_add_properties)
except Exception as e:
    raise TGAIllegalDataException(e)

eventProperties = {
    "#time": datetime.datetime.now(),
    "#ip": "123.123.123.123",
    "age": 18,
    "name": "hello",
    "array": ["string1", "🙂", "😀"],
    "dict": {
        "name": "world",
        "time": datetime.datetime.now(),
        "arrayString": ["aa", "bb", "cc"],
        "arrayNumber": [1, 2.0, 3333.4444],
        "isDog": False,
        "tupleA": ('t_a', 't_2'),
        "dictA": {"key1": "value1", "key2": "value2"},
    },
    "timeDict": {
        "t_key": datetime.datetime.now(),
        "t_key2": datetime.date.today(),
        't_key3': [
            datetime.datetime.now(),
            datetime.date.today(),
            {"child_t_key": datetime.date.today()},
        ],
    }
}

try:
    te.track(account_id=account_id, distinct_id=distinct_id, event_name="a", properties=eventProperties)
except Exception as e:
    raise TGAIllegalDataException(e)


class DynamicPropertiesTracker(DynamicSuperPropertiesTracker):
    def get_dynamic_super_properties(self):
        return {'super_dynamic_key': datetime.datetime.now()}


te.set_dynamic_super_properties_tracker(DynamicPropertiesTracker())
te.set_super_properties({"super_key_1": "value_1"})

try:
    te.track(account_id=account_id,
             distinct_id=distinct_id,
             event_name="track_with_super_property",
             properties=eventProperties)
except Exception as e:
    raise TGAIllegalDataException(e)

te.clear_super_properties()
te.set_dynamic_super_properties_tracker(None)

try:
    te.track(account_id=account_id,
             distinct_id=distinct_id,
             event_name="track_without_super_property",
             properties=eventProperties)
except Exception as e:
    raise TGAIllegalDataException(e)

try:
    te.track_first(account_id=account_id,
                   distinct_id=distinct_id,
                   event_name='first_event',
                   first_check_id='9999abc',
                   properties=eventProperties)
except Exception as e:
    raise TGAIllegalDataException(e)

eventProperties_1 = {
    "age": 18,
    "name": "hello",
}

try:
    te.track(account_id=account_id,
             distinct_id=distinct_id,
             event_name="track_before_update",
             properties=eventProperties)
except Exception as e:
    raise TGAIllegalDataException(e)

eventProperties_1_update = {
    "age": 88,
}

try:
    te.track_update(account_id=account_id,
                    distinct_id=distinct_id,
                    event_name='update_age',
                    event_id='123',
                    properties=eventProperties_1_update)
except Exception as e:
    raise TGAIllegalDataException(e)

eventProperties_1_overwrite = {
    "age": 88,
}

try:
    te.track_overwrite(account_id=account_id,
                       distinct_id=distinct_id,
                       event_name='overwrite',
                       event_id='123',
                       properties=eventProperties_1_overwrite)
except Exception as e:
    raise TGAIllegalDataException(e)

te.flush()
te.close()
