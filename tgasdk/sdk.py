# encoding:utf-8

from __future__ import unicode_literals

import datetime
import gzip
import json
import os
import re
import threading
import time
import uuid

import requests
from requests import ConnectionError

try:
    import queue
    from urllib.parse import urlparse
except ImportError:
    import Queue as queue
    from urlparse import urlparse
try:
    isinstance("", basestring)


    def is_str(s):
        return isinstance(s, basestring)
except NameError:
    def is_str(s):
        return isinstance(s, str)
try:
    isinstance(1, long)


    def is_int(n):
        return isinstance(n, int) or isinstance(n, long)
except NameError:
    def is_int(n):
        return isinstance(n, int)

try:
    from enum import Enum

    ROTATE_MODE = Enum('ROTATE_MODE', ('DAILY', 'HOURLY'))
except ImportError:
    class ROTATE_MODE(object):
        DAILY = 0
        HOURLY = 1


class TGAException(Exception):
    pass


class TGAIllegalDataException(TGAException):
    """数据格式异常

    在发送的数据格式有误时，SDK 会抛出此异常，用户应当捕获并处理.
    """
    pass


class TGANetworkException(TGAException):
    """网络异常

    在因为网络或者不可预知的问题导致数据无法发送时，SDK会抛出此异常，用户应当捕获并处理.
    """
    pass


__version__ = '1.8.0'

class DynamicSuperPropertiesTracker():
    def get_dynamic_super_properties(self):
        raise NotImplementedError

class TGAnalytics(object):
    """TGAnalytics 实例是发送事件数据和用户属性数据的关键实例
    """

    __NAME_PATTERN = re.compile(r"^(#[a-z][a-z0-9_]{0,49})|([a-z][a-z0-9_]{0,50})$", re.I)

    def __init__(self, consumer, enable_uuid=False):
        """创建一个 TGAnalytics 实例

        TGAanlytics 需要与指定的 Consumer 一起使用，可以使用以下任何一种:
        - LoggingConsumer: 批量实时写本地文件，并与 LogBus 搭配
        - BatchConsumer: 批量实时地向TA服务器传输数据（同步阻塞），不需要搭配传输工具
        - AsyncBatchConsumer: 批量实时地向TA服务器传输数据（异步非阻塞），不需要搭配传输工具
        - DebugConsumer: 逐条发送数据，并对数据格式做严格校验

        Args:
            consumer: 指定的 Consumer
        """
        self.__consumer = consumer
        self.__enableUuid = enable_uuid
        self.__super_properties = {}
        self.clear_super_properties()
        self.__dynamic_super_properties_tracker = None;

    def set_dynamic_super_properties_tracker(self, dynamic_super_properties_tracker):
        self.__dynamic_super_properties_tracker = dynamic_super_properties_tracker

    def user_set(self, distinct_id=None, account_id=None, properties=None):
        """设置用户属性

        对于一般的用户属性，您可以调用 user_set 来进行设置。使用该接口上传的属性将会覆盖原有的属性值，如果之前不存在该用户属性，
        则会新建该用户属性，类型与传入属性的类型一致.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            properties: dict 类型的用户属性
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_set', properties_add=properties)

    def user_unset(self, distinct_id=None, account_id=None, properties=None):
        """
        删除某个用户的用户属性
        :param distinct_id:
        :param account_id:
        :param properties:
        """
        if isinstance(properties, list):
            properties = dict((key, 0) for key in properties)
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_unset', properties_add=properties)

    def user_setOnce(self, distinct_id=None, account_id=None, properties=None):
        """设置用户属性, 不覆盖已存在的用户属性

        如果您要上传的用户属性只要设置一次，则可以调用 user_setOnce 来进行设置，当该属性之前已经有值的时候，将会忽略这条信息.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            properties: dict 类型的用户属性
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_setOnce', properties_add=properties)

    def user_add(self, distinct_id=None, account_id=None, properties=None):
        """对指定的数值类型的用户属性进行累加操作

        当您要上传数值型的属性时，您可以调用 user_add 来对该属性进行累加操作. 如果该属性还未被设置，则会赋值0后再进行计算.
        可传入负值，等同于相减操作.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            properties: 数值类型的用户属性
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_add', properties_add=properties)

    def user_append(self, distinct_id=None, account_id=None, properties=None):
        """追加一个用户的某一个或者多个集合类型
        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            properties: 集合
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_append', properties_add=properties)

    def user_uniq_append(self, distinct_id=None, account_id=None, properties=None):
        """追加一个用户的某一个或者多个集合类型, 能对集合内数据去重
        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            properties: 集合
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_uniq_append', properties_add=properties)

    def user_del(self, distinct_id=None, account_id=None):
        """删除用户

        如果您要删除某个用户，可以调用 user_del 将该用户删除。调用此函数后，将无法再查询该用户的用户属性, 但该用户产生的事件仍然可以被查询到.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_del')

    def track(self, distinct_id=None, account_id=None, event_name=None, properties=None):
        """发送事件数据

        您可以调用 track 来上传事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的名称只能以字母开头，可包含数字，字母和下划线“_”，
        长度最大为 50 个字符，对字母大小写不敏感. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            event_name: 事件名称
            properties: 事件属性

        Raises:
            TGAIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name)

        if properties:
            all_properties.update(properties)

        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track', event_name=event_name,
                   properties_add=all_properties)

    def track_first(self, distinct_id=None, account_id=None, event_name=None, first_check_id=None, properties=None):
        """发送首次事件数据

        您可以调用 track_first 来上传首次事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.
        首次事件是指针对某个设备或者其他维度的 ID，只会记录一次的事件. 例如在一些场景下，您可能希望记录在某个设备上第一次发生的事件，则可以用首次事件来上报数据.
        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            event_name: 事件名称
            first_check_id: 首次事件维度ID
            properties: 事件属性

        Raises:
            TGAIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name)

        if properties:
            all_properties.update(properties)

        if first_check_id:
            all_properties.update({'#first_check_id':first_check_id})
        else:
            all_properties.update({'#first_check_id':distinct_id})

        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track', event_name=event_name,
                   properties_add=all_properties)

    def track_update(self, distinct_id=None, account_id=None, event_name=None, event_id=None, properties=None):
        """发送可更新的事件数据

        您可以调用 track_update 来上传可更新的事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的名称只能以字母开头，可包含数字，字母和下划线“_”，
        长度最大为 50 个字符，对字母大小写不敏感. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            event_name: 事件名称
            event_id: 事件唯一ID
            properties: 事件属性

        Raises:
            TGAIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name)

        if properties:
            all_properties.update(properties)

        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track_update', event_name=event_name,
                   event_id=event_id, properties_add=all_properties)

    def track_overwrite(self, distinct_id=None, account_id=None, event_name=None, event_id=None, properties=None):
        """发送可覆盖的事件数据

        您可以调用 track_overwrite 来上传可全部覆盖的事件，建议您根据先前梳理的文档来设置事件的属性以及发送信息的条件. 事件的名称只能以字母开头，可包含数字，字母和下划线“_”，
        长度最大为 50 个字符，对字母大小写不敏感. 事件的属性是一个 dict 对象，其中每个元素代表一个属性.

        Args:
            distinct_id: 访客 ID
            account_id: 账户 ID
            event_name: 事件名称
            event_id: 事件唯一ID
            properties: 事件属性

        Raises:
            TGAIllegalDataException: 数据格式错误时会抛出此异常
        """
        all_properties = self._public_track_add(event_name)

        if properties:
            all_properties.update(properties)

        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track_overwrite', event_name=event_name,
                   event_id=event_id, properties_add=all_properties)

    def flush(self):
        """立即提交数据到相应的接收端
        """
        self.__consumer.flush()

    def close(self):
        """关闭并退出 sdk

        请在退出前调用本接口，以避免缓存内的数据丢失
        """
        self.__consumer.close()

    def _public_track_add(self, event_name):
        if not is_str(event_name):
            raise TGAIllegalDataException('a string type event_name is required for track')

        all_properties = {
            '#lib': 'tga_python_sdk',
            '#lib_version': __version__,
        }
        all_properties.update(self.__super_properties)
        if self.__dynamic_super_properties_tracker:
            all_properties.update(self.__dynamic_super_properties_tracker.get_dynamic_super_properties())
        return all_properties
        pass

    def __add(self, distinct_id, account_id, send_type, event_name=None, event_id=None, properties_add=None):
        if distinct_id is None and account_id is None:
            raise TGAException("Distinct_id and account_id must be set at least one")

        if properties_add:
            properties = properties_add.copy()
        else:
            properties = {}
        data = {
            '#type': send_type
        }
        if "#ip" in properties.keys():
            data['#ip'] = properties.get("#ip")
            del (properties['#ip'])
        if "#first_check_id" in properties.keys():
            data['#first_check_id'] = properties.get("#first_check_id")
            del (properties['#first_check_id'])
        # 只支持UUID标准格式xxxxxxxx - xxxx - xxxx - xxxx - xxxxxxxxxxxx
        if "#uuid" in properties.keys():
            data['#uuid'] = str(properties['#uuid'])
            del (properties['#uuid'])
        elif self.__enableUuid:
            data['#uuid'] = str(uuid.uuid1())
        if "#app_id" in properties.keys():
            data['#app_id'] = properties.get("#app_id")
            del (properties['#app_id'])

        self.__assert_properties(send_type, properties)
        td_time = properties.get("#time")
        data['#time'] = td_time
        del (properties['#time'])

        data['properties'] = properties

        if event_name is not None:
            data['#event_name'] = event_name
        if event_id is not None:
            data['#event_id'] = event_id
        if distinct_id is not None:
            data['#distinct_id'] = distinct_id
        if account_id is not None:
            data['#account_id'] = account_id

        self.__consumer.add(json.dumps(data))

    def __assert_properties(self, action_type, properties):
        if properties is not None:
            if "#time" not in properties.keys():
                properties['#time'] = datetime.datetime.now()
            else:
                try:
                    time_temp = properties.get('#time')
                    if isinstance(time_temp, datetime.datetime) or isinstance(time_temp, datetime.date):
                        pass
                    else:
                        raise TGAIllegalDataException('Value of #time should be datetime.datetime or datetime.date')
                except Exception as e:
                    raise TGAIllegalDataException(e)

            for key, value in properties.items():
                if not is_str(key):
                    raise TGAIllegalDataException("Property key must be a str. [key=%s]" % str(key))

                if value is None:
                    continue

                if not self.__NAME_PATTERN.match(key):
                    raise TGAIllegalDataException(
                        "type[%s] property key must be a valid variable name. [key=%s]" % (action_type, str(key)))

                if 'user_add' == action_type.lower() and not self.__number(value) and not key.startswith('#'):
                    raise TGAIllegalDataException('user_add properties must be number type')

                if isinstance(value, datetime.datetime):
                    properties[key] = value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                elif isinstance(value, datetime.date):
                    properties[key] = value.strftime('%Y-%m-%d')
                if isinstance(value, list):
                    i = 0
                    for lvalue in value:
                        if isinstance(lvalue, datetime.datetime):
                            value[i] = lvalue.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        i += 1

                if isinstance(value, dict):
                    for k, v in value.items():
                        if isinstance(v, datetime.datetime):
                            value[k] = v.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        elif isinstance(v, datetime.date):
                            value[k] = v.strftime('%Y-%m-%d')

    def __number(self, s):
        if is_int(s):
            return True
        if isinstance(s, float):
            return True
        return False

    def clear_super_properties(self):
        """删除所有已设置的事件公共属性
        """
        self.__super_properties = {
            '#lib': 'tga_python_sdk',
            '#lib_version': __version__,
        }

    def set_super_properties(self, super_properties):
        """设置公共事件属性

        公共事件属性是所有事件中的属性属性，建议您在发送事件前，先设置公共事件属性. 当 track 的 properties 和
        super properties 有相同的 key 时，track 的 properties 会覆盖公共事件属性的值.

        Args:
            super_properties 公共属性
        """
        self.__super_properties.update(super_properties)


if os.name == 'nt':
    import msvcrt


    def _lock(file_):
        try:
            save_pos = file_.tell()
            file_.seek(0)
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_LOCK, 1)
            except IOError as e:
                raise TGAException(e)
            finally:
                if save_pos:
                    file_.seek(save_pos)
        except IOError as e:
            raise TGAException(e)


    def _unlock(file_):
        try:
            save_pos = file_.tell()
            if save_pos:
                file_.seek(0)
            try:
                msvcrt.locking(file_.fileno(), msvcrt.LK_UNLCK, 1)
            except IOError as e:
                raise TGAException(e)
            finally:
                if save_pos:
                    file_.seek(save_pos)
        except IOError as e:
            raise TGAException(e)
elif os.name == 'posix':
    import fcntl


    def _lock(file_):
        try:
            fcntl.flock(file_.fileno(), fcntl.LOCK_EX)
        except IOError as e:
            raise TGAException(e)


    def _unlock(file_):
        fcntl.flock(file_.fileno(), fcntl.LOCK_UN)
else:
    raise TGAException("Python SDK is defined for NT and POSIX system.")


class _TAFileLock(object):
    def __init__(self, file_handler):
        self._file_handler = file_handler

    def __enter__(self):
        _lock(self._file_handler)
        return self

    def __exit__(self, t, v, tb):
        _unlock(self._file_handler)

class LoggingConsumer(object):
    """数据批量实时写入本地文件

    创建指定文件存放目录的 LoggingConsumer, 将数据使用 logging 库输出到指定路径. 同时，需将 LogBus 的监听文件夹地址
    设置为此处的地址，即可使用LogBus进行数据的监听上传.
    """
    _mutex = queue.Queue()
    _mutex.put(1)

    class _FileWriter(object):
        _writers = {}
        _writeMutex = queue.Queue()
        _writeMutex.put(1)

        @classmethod
        def instance(cls, filename):
            cls._writeMutex.get(block=True, timeout=None)
            try:
                if filename in cls._writers.keys():
                    result = cls._writers[filename]
                    result._count = result._count + 1
                else:
                    result = cls(filename)
                    cls._writers[filename] = result
                return result
            finally:
                cls._writeMutex.put(1)

        def __init__(self, filename):
            self._filename = filename
            self._file = open(self._filename, 'a')
            self._count = 1

        def close(self):
            LoggingConsumer._FileWriter._writeMutex.get(block=True, timeout=None)
            try:
                self._count = self._count - 1
                if self._count == 0:
                    self._file.close()
                    del LoggingConsumer._FileWriter._writers[self._filename]
            finally:
                LoggingConsumer._FileWriter._writeMutex.put(1)

        def is_valid(self, filename):
            return self._filename == filename

        def write(self, messages):
            with _TAFileLock(self._file):
                for message in messages:
                    self._file.write(message)
                    self._file.write('\n')
                self._file.flush()

    @classmethod
    def construct_filename(cls, directory, date_suffix, file_size, file_prefix):
        filename = file_prefix + ".log." + date_suffix \
            if file_prefix is not None else "log." + date_suffix

        if file_size > 0:
            count = 0
            file_path = directory + filename + "_" + str(count)
            while os.path.exists(file_path) and cls.file_size_out(file_path, file_size):
                count = count + 1
                file_path = directory + filename + "_" + str(count)
            return file_path
        else:
            return directory + filename

    @classmethod
    def file_size_out(cls, file_path, file_size):
        fsize = os.path.getsize(file_path)
        fsize = fsize / float(1024 * 1024)
        if fsize >= file_size:
            return True
        return False

    @classmethod
    def unlock_logging_consumer(cls):
        cls._mutex.put(1)

    @classmethod
    def lock_logging_consumer(cls):
        cls._mutex.get(block=True, timeout=None)

    def __init__(self, log_directory, log_size=0, buffer_size=5, rotate_mode=ROTATE_MODE.DAILY, file_prefix=None):
        """创建指定日志文件目录的 LoggingConsumer

        Args:
            log_directory: 日志保存目录
            log_size: 单个日志文件的大小, 单位 MB, log_size <= 0 表示不限制单个文件大小
            buffer_size: 每次写入文件数据量, 默认 5 条写入一次
            rotate_mode: 日志切分模式，默认按天切分
        """
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        self.log_directory = log_directory  # log文件保存的目录
        self.sdf = '%Y-%m-%d-%H' if rotate_mode == ROTATE_MODE.HOURLY else '%Y-%m-%d'
        self.suffix = datetime.datetime.now().strftime(self.sdf)
        self._fileSize = log_size  # 单个log文件的大小
        if not self.log_directory.endswith("/"):
            self.log_directory = self.log_directory + "/"

        self._buffer = []
        self._buffer_size = buffer_size
        self._file_prefix = file_prefix
        self.lock_logging_consumer()
        filename = LoggingConsumer.construct_filename(self.log_directory, self.suffix, self._fileSize,
                                                      self._file_prefix)
        self._writer = LoggingConsumer._FileWriter.instance(filename)
        self.unlock_logging_consumer()

    def add(self, msg):
        messages = None
        self.lock_logging_consumer()
        self._buffer.append(msg)
        if len(self._buffer) > self._buffer_size:
            messages = self._buffer
            self.refresh_writer()
            self._buffer = []
        if messages:
            self._writer.write(messages)
        self.unlock_logging_consumer()

    def flush_with_close(self, is_close):
        messages = None
        self.lock_logging_consumer()
        if len(self._buffer) > 0:
            messages = self._buffer
            self.refresh_writer()
            self._buffer = []
        if messages:
            self._writer.write(messages)
        if is_close:
            self._writer.close()
        self.unlock_logging_consumer()

    def refresh_writer(self):
        date_suffix = datetime.datetime.now().strftime(self.sdf)
        if self.suffix != date_suffix:
            self.suffix = date_suffix
        filename = LoggingConsumer.construct_filename(self.log_directory, self.suffix, self._fileSize,
                                                      self._file_prefix)
        if not self._writer.is_valid(filename):
            self._writer.close()
            self._writer = LoggingConsumer._FileWriter.instance(filename)

    def flush(self):
        self.flush_with_close(False)

    def close(self):
        self.flush_with_close(True)


class BatchConsumer(object):
    """同步、批量地向 TA 服务器传输数据

    通过指定接收端地址和 APP ID，可以同步的向 TA 服务器传输数据. 此 Consumer 不需要搭配传输工具,
    但是存在网络不稳定等原因造成数据丢失的可能，因此不建议在生产环境中使用.

    触发上报的时机为以下条件满足其中之一的时候:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """
    _batchlock = threading.RLock()
    _cachelock = threading.RLock()

    def __init__(self, server_uri, appid, batch=20, timeout=30000, interval=3, compress=True, max_cache_size=50):
        """创建 BatchConsumer

        Args:
            server_uri: 服务器的 URL 地址
            appid: 项目的 APP ID
            batch: 指定触发上传的数据条数, 默认为 20 条, 最大 200 条
            timeout: 请求的超时时间, 单位毫秒, 默认为 30000 ms
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
        """
        self.__interval = interval
        self.__batch = min(batch, 200)
        self.__message_channel = []
        self.__max_cache_size = max_cache_size
        self.__cache_buffer = []
        self.__last_flush = time.time()
        server_url = urlparse(server_uri)
        self.__http_service = _HttpServices(server_url._replace(path='/sync_server').geturl(), appid, timeout)
        self.__http_service.compress = compress

    def add(self, msg):
        self._batchlock.acquire()
        try:
            self.__message_channel.append(msg)
        finally:
            self._batchlock.release()
        if len(self.__message_channel) >= self.__batch \
                or len(self.__cache_buffer) > 0:
            self.flush_once()

    def flush(self, throw_exception=True):
        while len(self.__cache_buffer) > 0 or len(self.__message_channel) > 0:
            try:
                self.flush_once(throw_exception)
            except TGAIllegalDataException:
                continue

    def flush_once(self, throw_exception=True):
        if len(self.__message_channel) == 0 and len(self.__cache_buffer) == 0:
            return

        self._cachelock.acquire()
        self._batchlock.acquire()
        try:
            try:
                if len(self.__message_channel) == 0 and len(self.__cache_buffer) == 0:
                    return
                if len(self.__cache_buffer) == 0 or len(self.__message_channel) >= self.__batch:
                    self.__cache_buffer.append(self.__message_channel)
                    self.__message_channel = []
            finally:
                self._batchlock.release()
            msg = self.__cache_buffer[0]
            self.__http_service.send('[' + ','.join(msg) + ']', str(len(msg)))
            self.__last_flush = time.time()
            self.__cache_buffer = self.__cache_buffer[1:]
        except TGANetworkException as e:
            if throw_exception:
                raise e
        except TGAIllegalDataException as e:
            self.__cache_buffer = self.__cache_buffer[1:]
            if throw_exception:
                raise e
        finally:
            if len(self.__cache_buffer) > self.__max_cache_size:
                self.__cache_buffer = self.__cache_buffer[1:]
            self._cachelock.release()

    def close(self):
        self.flush()

    pass


class AsyncBatchConsumer(object):
    """异步、批量地向 TA 服务器发送数据的

    AsyncBatchConsumer 使用独立的线程进行数据发送，当满足以下两个条件之一时触发数据上报:
    1. 数据条数大于预定义的最大值, 默认为 20 条
    2. 数据发送间隔超过预定义的最大时间, 默认为 3 秒
    """

    def __init__(self, server_uri, appid, interval=3, flush_size=20, queue_size=100000):
        """创建 AsyncBatchConsumer

        Args:
            server_uri: 服务器的 URL 地址
            appid: 项目的 APP ID
            interval: 推送数据的最大时间间隔, 单位为秒, 默认为 3 秒
            flush_size: 队列缓存的阈值，超过此值将立即进行发送
            queue_size: 缓存队列的大小
        """
        server_url = urlparse(server_uri)
        self.__http_service = _HttpServices(server_url._replace(path='/sync_server').geturl(), appid, 30000)
        self.__batch = flush_size
        self.__queue = queue.Queue(queue_size)

        # 初始化发送线程
        self.__flushing_thread = self._AsyncFlushThread(self, interval)
        self.__flushing_thread.daemon = True
        self.__flushing_thread.start()

    def add(self, msg):
        try:
            self.__queue.put_nowait(msg)
        except queue.Full as e:
            raise TGANetworkException(e)

        if self.__queue.qsize() > self.__batch:
            self.flush()

    def flush(self):
        self.__flushing_thread.flush()

    def close(self):
        self.__flushing_thread.stop()
        while not self.__queue.empty():
            self._perform_request()

    def _perform_request(self):
        """同步的发送数据

        仅用于内部调用, 用户不应当调用此方法.
        """
        flush_buffer = []
        while len(flush_buffer) < self.__batch:
            try:
                flush_buffer.append(str(self.__queue.get_nowait()))
            except queue.Empty:
                break

        if len(flush_buffer) > 0:
            for i in range(3):  # 网络异常情况下重试 3 次
                try:
                    self.__http_service.send('[' + ','.join(flush_buffer) + ']', str(len(flush_buffer)))
                    return True
                except TGANetworkException:
                    pass
                except TGAIllegalDataException:
                    break

    class _AsyncFlushThread(threading.Thread):
        def __init__(self, consumer, interval):
            threading.Thread.__init__(self)
            self._consumer = consumer
            self._interval = interval

            self._stop_event = threading.Event()
            self._finished_event = threading.Event()
            self._flush_event = threading.Event()

        def flush(self):
            self._flush_event.set()

        def stop(self):
            """停止线程

            退出时需调用此方法，以保证线程安全结束.
            """
            self._stop_event.set()
            self._finished_event.wait()

        def run(self):
            while True:
                # 如果 _flush_event 标志位为 True，或者等待超过 _interval 则继续执行
                self._flush_event.wait(self._interval)
                self._consumer._perform_request()
                self._flush_event.clear()

                # 发现 stop 标志位时安全退出
                if self._stop_event.isSet():
                    break
            self._finished_event.set()


def _gzip_string(data):
    try:
        return gzip.compress(data)
    except AttributeError:
        import StringIO
        buf = StringIO.StringIO()
        fd = gzip.GzipFile(fileobj=buf, mode="w")
        fd.write(data)
        fd.close()
        return buf.getvalue()


class _HttpServices(object):
    """内部类，用于发送网络请求

    指定接收端地址和项目 APP ID, 实现向接收端上传数据的接口. 发送前将数据默认使用 Gzip 压缩,
    """

    def __init__(self, server_uri, appid, timeout=30000):
        self.url = server_uri
        self.appid = appid
        self.timeout = timeout
        self.compress = True

    def send(self, data, length):
        """使用 Requests 发送数据给服务器

        Args:
            data: 待发送的数据
            length

        Raises:
            TGAIllegalDataException: 数据错误
            TGANetworkException: 网络错误
        """
        headers = {'appid': self.appid, 'TA-Integration-Type': 'python-sdk', 'TA-Integration-Version': __version__,
                   'TA-Integration-Count': length}
        try:
            compress_type = 'gzip'
            if self.compress:
                data = _gzip_string(data.encode("utf-8"))
            else:
                compress_type = 'none'
                data = data.encode("utf-8")
            headers['compress'] = compress_type
            response = requests.post(self.url, data=data, headers=headers, timeout=self.timeout)
            if response.status_code == 200:
                responseData = json.loads(response.text)
                if responseData["code"] == 0:
                    return True
                else:
                    raise TGAIllegalDataException("Unexpected result code: " + str(responseData["code"]))
            else:
                raise TGANetworkException("Unexpected Http status code " + str(response.status_code))
        except ConnectionError as e:
            time.sleep(0.5)
            raise TGANetworkException("Data transmission failed due to " + repr(e))


class DebugConsumer(object):
    """逐条、同步的发送数据给接收服务器

    服务端会对数据进行严格校验，当某个属性不符合规范时，整条数据都不会入库. 当数据格式错误时抛出包含详细原因的异常信息.
    建议首先使用此 Consumer 来调试埋点数据.
    """

    def __init__(self, server_uri, appid, timeout=30000, write_data=True):
        """创建 DebugConsumer

        Args:
            server_uri: 服务器的 URL 地址
            appid: 项目的 APP ID
            timeout: 请求的超时时间, 单位毫秒, 默认为 30000 ms
        """
        server_url = urlparse(server_uri)
        debug_url = server_url._replace(path='/data_debug')
        self.__server_uri = debug_url.geturl()
        self.__appid = appid
        self.__timeout = timeout
        self.__writer_data = write_data

    def add(self, msg):
        try:
            dry_run = 0
            if not self.__writer_data:
                dry_run = 1
            response = requests.post(self.__server_uri,
                                     data={'source': 'server', 'appid': self.__appid, 'data': msg, 'dryRun': dry_run},
                                     timeout=self.__timeout)
            if response.status_code == 200:
                responseData = json.loads(response.text)
                if responseData["errorLevel"] == 0:
                    return True
                else:
                    print("Unexpected result : \n %s" % response.text)
            else:
                raise TGANetworkException("Unexpected http status code: " + str(response.status_code))
        except ConnectionError as e:
            time.sleep(0.5)
            raise TGANetworkException("Data transmission failed due to " + repr(e))

    def flush(self, throw_exception=True):
        pass

    def close(self):
        pass
