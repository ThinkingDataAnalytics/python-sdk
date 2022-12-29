# encoding:utf-8

from __future__ import unicode_literals
from ast import IsNot

import datetime
import gzip
import json
import os
import re
import threading
import time
import uuid
from matplotlib.colors import cnames

import requests
from requests import ConnectionError
from sqlalchemy import false


__NAME_PATTERN = re.compile(r"^[#a-zA-Z][a-zA-Z0-9_]{0,49}$", re.I)

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


def isNumber(s):
    if is_int(s):
         return True
    if isinstance(s, float):
        return True
    return False


def  assert_properties(action_type, properties):
        if properties is not None:
            if "#time" in properties.keys():
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

                if not __NAME_PATTERN.match(key):
                    raise TGAIllegalDataException(
                        "type[%s] property key must be a valid variable name. [key=%s]" % (action_type, str(key)))

                if 'user_add' == action_type.lower() and not isNumber(value) and not key.startswith('#'):
                    raise TGAIllegalDataException('user_add properties must be number type')
                    

__version__ = '2.1.0'
is_print = False
def log(msg=None):
    if (msg is not None and is_print ) :
        print('[ThinkingAnalytics-Python SDK V%s]-%s' %(__version__,msg))


class TGAException(Exception):
    pass


class TGAIllegalDataException(TGAException):
    pass


class TGANetworkException(TGAException):
    pass



class DynamicSuperPropertiesTracker():
    def get_dynamic_super_properties(self):
        raise NotImplementedError

class TADateTimeSerializer(json.JSONEncoder):
       
        def default(self, obj):
            if isinstance(obj, datetime.datetime):
                head_fmt = "%Y-%m-%d %H:%M:%S"
                return obj.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            elif isinstance(obj, datetime.date):
                fmt = '%Y-%m-%d'
                return obj.strftime(fmt)
            return json.JSONEncoder.default(self, obj)

class TGAnalytics(object):
    __strict = False

    def __init__(self, consumer, enable_uuid=False,strict=None):
        """
        - LoggingConsumer: Write local files in batches
        - BatchConsumer: Transfer data to the TE server in batches(synchronous blocking)
        - AsyncBatchConsumer:Transfer data to the TE server in batches(asynchronous blocking)
        - DebugConsumer: Send data one by one, and strictly verify the data format 
        Args:
            consumer: required 
        """
     
        self.__consumer = consumer
        if isinstance(consumer,DebugConsumer):
            self.__strict = True
        if strict != None:
            self.__strict = strict
        
        self.__enableUuid = enable_uuid
        self.__super_properties = {}
        self.clear_super_properties()
        self.__dynamic_super_properties_tracker = None;

    def set_dynamic_super_properties_tracker(self, dynamic_super_properties_tracker):
        self.__dynamic_super_properties_tracker = dynamic_super_properties_tracker

    def user_set(self, distinct_id=None, account_id=None, properties=None):
        """
        Args:
            distinct_id: string
            account_id: string
            properties: dict 
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_set', properties_add=properties)

    def user_unset(self, distinct_id=None, account_id=None, properties=None):
        """
        param distinct_id:string
        param account_id:string
        param properties:dic
        """
        if isinstance(properties, list):
            properties = dict((key, 0) for key in properties)
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_unset', properties_add=properties)

    def user_setOnce(self, distinct_id=None, account_id=None, properties=None):
        """

        if the user property you want to upload only needs to be set once, you can call user_setOnce to set it. When the property has value before, this item will be ignored.

        Args:
            distinct_id: string
            account_id: string
            properties: dict
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_setOnce', properties_add=properties)

    def user_add(self, distinct_id=None, account_id=None, properties=None):
        """
        Args:
            distinct_id: string
            account_id: string  
            properties: dic
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_add', properties_add=properties)

    def user_append(self, distinct_id=None, account_id=None, properties=None):
        """
        Args:
            distinct_id:string
            account_id: string
            properties: dic
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_append', properties_add=properties)

    def user_uniq_append(self, distinct_id=None, account_id=None, properties=None):
        """
        Args:
            distinct_id:string
            account_id:string
            properties: dic
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_uniq_append', properties_add=properties)

    def user_del(self, distinct_id=None, account_id=None):
        """
        Args:
            distinct_id:string
            account_id:string
        """
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='user_del')

    def track(self, distinct_id=None, account_id=None, event_name=None, properties=None):
        """
        Args:
            distinct_id: string
            account_id: string
            event_name: string
            properties: dic

        Raises:
            TGAIllegalDataException
        """
        all_properties = self._public_track_add(event_name,properties)
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track', event_name=event_name,
                   properties_add=all_properties)

    def track_first(self, distinct_id=None, account_id=None, event_name=None, first_check_id=None, properties=None):
        """
        Args:
            distinct_id: string
            account_id: string
            event_name:string
            first_check_id: string
            properties: dic

        Raises:
            TGAIllegalDataException
        """
        all_properties = self._public_track_add(event_name,properties)
        if first_check_id is None and self.__strict:
            raise TGAException("first_check_id must be set")
        # if first_check_id:
        #     all_properties.update({'#first_check_id':first_check_id})
        # else:
        #     all_properties.update({'#first_check_id':distinct_id})

        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track',event_id=first_check_id,event_name=event_name,
                   properties_add=all_properties)

    def track_update(self, distinct_id=None, account_id=None, event_name=None, event_id=None, properties=None):
        """
        Args:
            distinct_id: string
            account_id: string
            event_name: string
            event_id: string
            properties: dic

        Raises:
            TGAIllegalDataException
        """
        if event_id is None and self.__strict:
            raise TGAException("event_id must be set")

        all_properties = self._public_track_add(event_name,properties)
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track_update', event_name=event_name,
                   event_id=event_id, properties_add=all_properties)

    def track_overwrite(self, distinct_id=None, account_id=None, event_name=None, event_id=None, properties=None):
        """
        Args:
            distinct_id: string
            account_id: string
            event_name: string
            event_id: string
            properties: dic

        Raises:
            TGAIllegalDataException
        """
        if event_id is None and self.__strict:
                raise TGAException("event_id must be set")
        all_properties = self._public_track_add(event_name,properties)
        self.__add(distinct_id=distinct_id, account_id=account_id, send_type='track_overwrite', event_name=event_name,
                   event_id=event_id, properties_add=all_properties)

    def flush(self):
        """
        upload data immediately
        """
        self.__consumer.flush()

    def close(self):
        """
        Please call this api before exiting to avoid data loss in the cache
        """
        self.__consumer.close()

    def _public_track_add(self,event_name,properties):
        if not is_str(event_name):
            raise TGAIllegalDataException('a string type event_name is required for track')

        all_properties = {
            '#lib': 'tga_python_sdk',
            '#lib_version': __version__,
        }
        all_properties.update(self.__super_properties)
        if self.__dynamic_super_properties_tracker:
            all_properties.update(self.__dynamic_super_properties_tracker.get_dynamic_super_properties())
        if properties:
            all_properties.update(properties)
        return all_properties
        pass

    def __add(self, distinct_id, account_id, send_type, event_name=None, event_id=None, properties_add=None):
        if distinct_id is None and account_id is None:
            raise TGAException("Distinct_id and account_id must be set at least one")
        data = {'#type': send_type}    
        if send_type.find("track") !=-1 and event_id is not None:
            if send_type == "track":
              self.__buildData(data,'#first_check_id',event_id) 
            else: 
              self.__buildData(data,'#event_id',event_id)    

        if properties_add:
            properties = properties_add.copy()
        else:
            properties = {}
        self.__movePresetProperties(["#ip","#first_check_id","#app_id","#time",'#uuid'],data,properties)
        if self.__strict == True:
            assert_properties(send_type, properties)
        # 只支持UUID标准格式xxxxxxxx - xxxx - xxxx - xxxx - xxxxxxxxxxxx
        if ( self.__enableUuid and "#uuid" not in data.keys() ):
            data['#uuid'] = str(uuid.uuid1())
        if '#time' not in data:
            data['#time'] = datetime.datetime.now()
        self.__buildData(data,'#event_name',event_name)
        self.__buildData(data,'#distinct_id',distinct_id)
        self.__buildData(data,'#account_id',account_id)    
        data['properties'] = properties
        content = json.dumps(data,separators=(',', ':'),cls=TADateTimeSerializer)
        log("collect data={}".format(content))
        self.__consumer.add(content)
        
    def __buildData(self,data,key,value):
         if value is not None:
                data[key] = value

    def __movePresetProperties(self,keys,data,properties):
        for key in keys:
            if key in properties.keys():
                data[key] = properties.get(key)
                del (properties[key])


    def clear_super_properties(self):
        self.__super_properties = {
            '#lib': 'tga_python_sdk',
            '#lib_version': __version__ ,
        }

    def set_super_properties(self, super_properties):
        """
        Args:
            super_properties:string
        """
        self.__super_properties.update(super_properties)
    
    @staticmethod 
    def enableLog(isPrint=False):
        global is_print
        is_print = isPrint
    
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
    """Write data to local files in batches
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
        """
        Args:
            log_directory: The directory where the log files are saved
            log_size: The size of a single log file, in MB, log_size <= 0 means no limit on the size of a single file
            buffer_size: The amount of data written to the file each time, the default is to write 5 pieces at a time
            rotate_mode: Log splitting mode, by default splitting by day
        """
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        self.log_directory = log_directory
        self.sdf = '%Y-%m-%d-%H' if rotate_mode == ROTATE_MODE.HOURLY else '%Y-%m-%d'
        self.suffix = datetime.datetime.now().strftime(self.sdf)
        self._fileSize = log_size
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
    _batchlock = threading.RLock()
    _cachelock = threading.RLock()

    def __init__(self, server_uri, appid, batch=20, timeout=30000, interval=3, compress=True, max_cache_size=50):
        """
        Args:
            server_uri: 
            appid: 
            batch: Specify the number of data to trigger uploading, the default is 20, and the maximum is 200
            timeout: Request timeout, in milliseconds, default is 30000 ms
            interval: The maximum time interval for uploading data, in seconds, the default is 3 seconds
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

    def __init__(self, server_uri, appid, interval=3, flush_size=20, queue_size=100000):
        """

        Args:
            server_uri:
            appid: 
            interval: The maximum time interval for uploading data, in seconds, the default is 3 seconds
            flush_size: The threshold of the queue cache, if this value is exceeded, it will be sent immediately
            queue_size: The size of the storage queue
        """
        server_url = urlparse(server_uri)
        self.__http_service = _HttpServices(server_url._replace(path='/sync_server').geturl(), appid, 30000)
        self.__batch = flush_size
        self.__queue = queue.Queue(queue_size)

    
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
        self.flush()
        self.__flushing_thread.stop()
        while not self.__queue.empty():
            self._perform_request()

    def _perform_request(self):
    
        flush_buffer = []
        while len(flush_buffer) < self.__batch:
            try:
                flush_buffer.append(str(self.__queue.get_nowait()))
            except queue.Empty:
                break

        if len(flush_buffer) > 0:
            for i in range(3):  #Retry 3 times in case of network exception
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
            """
            Use of this method needs to be adjusted when exiting to ensure that the safeline program ends safely.
            """
            self._stop_event.set()
            self._finished_event.wait()

        def run(self):
            while True:
                self._flush_event.wait(self._interval)
                self._consumer._perform_request()
                self._flush_event.clear()
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
    def __init__(self, server_uri, appid, timeout=30000):
        self.url = server_uri
        self.appid = appid
        self.timeout = timeout
        self.compress = True

    def send(self, data, length):
        """
        Args:
            data:
            length
        Raises:
            TGAIllegalDataException: 
            TGANetworkException: network error
        """
        headers = {'appid': self.appid, 'TA-Integration-Type': 'python-sdk', 'TA-Integration-Version': __version__ ,
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
                log('response={}'.format(responseData))
                if responseData["code"] == 0:
                    return True
                else:
                    raise TGAIllegalDataException("Unexpected result code: " + str(responseData["code"]))
            else:
                log('response={}'.format(response.status_code))
                raise TGANetworkException("Unexpected Http status code " + str(response.status_code))
        except ConnectionError as e:
            time.sleep(0.5)
            raise TGANetworkException("Data transmission failed due to " + repr(e))


class DebugConsumer(object):
    """
    The server will strictly verify the data. When a certain attribute does not meet the specification, the entire data will not be stored. When the data format is wrong, an exception message containing detailed reasons will be thrown.
    It is recommended to use this Consumer first to debug buried point data.
    """

    def __init__(self, server_uri, appid, timeout=30000, write_data=True, device_id=""):
        """

        Args:
            server_uri: string
            appid: string
            timeout: Request timeout, in milliseconds, default is 30000 ms
            write_data: write data to TE or not
            device_id: debug device in TE
        """
        server_url = urlparse(server_uri)
        debug_url = server_url._replace(path='/data_debug')
        self.__server_uri = debug_url.geturl()
        self.__appid = appid
        self.__timeout = timeout
        self.__writer_data = write_data
        self.__device_id = device_id
        TGAnalytics.enableLog(True)

    def add(self, msg):
        try:
            dry_run = 0
            if not self.__writer_data:
                dry_run = 1

            params = {'source': 'server', 'appid': self.__appid, 'data': msg, 'dryRun': dry_run}

            if len(self.__device_id) > 0:
                params["deviceId"] = self.__device_id

            response = requests.post(self.__server_uri,
                                     data=params,
                                     timeout=self.__timeout)
            if response.status_code == 200:
                responseData = json.loads(response.text)
                log('response={}'.format(responseData))
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

    
