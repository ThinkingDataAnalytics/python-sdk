# coding=utf-8
import datetime
import re
from tkinter import NONE
import uuid
import os
import sys
import json
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
import unittest

from tgasdk.sdk import TGAException, TGAnalytics, BatchConsumer, AsyncBatchConsumer, LoggingConsumer, DebugConsumer, \
    TGAIllegalDataException, \
    ROTATE_MODE,assert_properties,isNumber

msg = {}
distinct_id = "hale"
account_id = "ta_001"
EventName = "eventName"
class SDKTest(object):
    __cases = []
    __verifies=[]
    def addCase(self,_case):
       self.__cases.append(_case)
    
    def addVerify(self,_verify):
        self.__verifies.append(_verify)
    
    def run(self):
        for _case in self.__cases:
            result = _case();
            for _verify in self.__verifies:
                _verify(result)

    def reset(self):
        self.__verifies = []
        self.__cases = []

class TestConsumer(object):
    def add(self, message):
        global msg
        msg=json.loads(message)
        print(msg)

    def flush(self, throw_exception=True):
        pass

    def close(self):
        pass


class BasicTest(unittest.TestCase):
    isPass  = True

    def setUp(self):
        self.sdk =  TGAnalytics(TestConsumer(),False,True);
        self.sdkTest = SDKTest()
        pass

    def setUpUserPropertyCase(self,distinctId=None,accountId=None,properties=None,result=None):
        def user_set():
            try:
                self.sdk.user_set(distinct_id=distinctId,account_id=accountId,properties=properties)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[0]
            return result

        def user_unSet():
            try:
                self.sdk.user_unset(distinct_id=distinctId,account_id=accountId,properties=['a'])
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[1]
            return result

        def user_setOnce():
            try:
                self.sdk.user_setOnce(distinct_id=distinctId,account_id=accountId,properties=properties)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[2]
            return result

        def user_append():
            try:
                self.sdk.user_append(distinct_id=distinctId,account_id=accountId,properties=properties)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[3]
            return result
        
        def user_uniq_append():
            try:
                self.sdk.user_uniq_append(distinct_id=distinctId,account_id=accountId,properties=properties)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[4]
            return result
        
        def user_delete():
            try:
                self.sdk.user_del(distinct_id=distinctId,account_id=accountId)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[5]
            return result
        
        def user_add():
            try:
                addProperties = {'a':1}
                for key,value in properties.items():
                    print(key)
                    if isNumber(value) or key.find('#') != -1:
                        addProperties[key] = value
                self.sdk.user_add(distinct_id=distinctId,account_id=accountId,properties=addProperties)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[6]
            return result
        
        self.sdkTest.addCase(user_set)
        self.sdkTest.addCase(user_unSet)
        self.sdkTest.addCase(user_setOnce)
        self.sdkTest.addCase(user_append)
        self.sdkTest.addCase(user_uniq_append)
        self.sdkTest.addCase(user_delete)
        self.sdkTest.addCase(user_add)
        

    def setUpEventCase(self,distinctId=None,accountId=None,eventName=None,properties=None,result=None):
        def track():
            try:
                self.sdk.track(distinct_id=distinctId,account_id=accountId,event_name=eventName,properties=properties)
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[0]
            return result

        def track_update():
            try:
                self.sdk.track_update(distinct_id=distinctId,account_id=accountId,event_name=eventName,properties=properties,event_id='event_id')
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[1]
            return result

        def track_overwrite():
            try:
                self.sdk.track_overwrite(distinct_id=distinctId,account_id=accountId,event_name=eventName,properties=properties,event_id='event_id')
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[2]
            return result

        def track_first():
            try:
                self.sdk.track_first(distinct_id=distinctId,account_id=accountId,event_name=eventName,properties=properties,first_check_id='first_check_id')
            except Exception as e:
                return e
            if result is not None and isinstance(result,list):
                return result[3]
            return result

        self.sdkTest.addCase(track)
        self.sdkTest.addCase(track_update)
        self.sdkTest.addCase(track_overwrite)
        self.sdkTest.addCase(track_first)
        

    #测试属性key的判定方法是否有效
    def test_assert_key_value(self):
        try:
            assert_properties("track",{"123":'123'});
            assert_properties("track",{"?123":'123'});
            assert_properties("track",{"{123":'123'});
            assert_properties("track",{"_123":'123'});
        except Exception as e:
            self.assertIsNotNone(e)

        try:
            assert_properties("track",{"a123":'123'});
            assert_properties("track",{"#123":'123'});
            assert_properties("track",{"#_123":'123'});
        except Exception as e:
            self.isPass = False
        self.assertTrue(self.isPass)

    def test_emptyAccountId_emptyDistinctId(self):
         def verify(result):
                self.assertTrue(isinstance(result,Exception))
                self.assertEqual("Distinct_id and account_id must be set at least one",result.args[0])
         self.setUpEventCase(eventName=EventName,properties={})
         self.setUpUserPropertyCase(properties={});
         self.sdkTest.addVerify(verify)
         self.sdkTest.run()
    
    def test_emptyDistinctId(self):
         self.sdkTest.reset();
         def verify(result):
                self.assertEqual(result,None) 
         self.setUpEventCase(accountId=account_id,eventName=EventName,properties={})
         self.setUpUserPropertyCase(accountId=account_id,properties={});
         self.sdkTest.addVerify(verify)
         self.sdkTest.run()
         
    
    def test_emptyAccountId(self):
         self.sdkTest.reset();
         def verify(result):
                self.assertEquals(result,None) 
         self.setUpEventCase(distinctId=distinct_id,eventName=EventName,properties={})
         self.setUpUserPropertyCase(accountId=account_id,properties={});
         self.sdkTest.addVerify(verify)
         self.sdkTest.run()
        
    
    def test_AccountId_distinctId(self):
         self.sdkTest.reset();
         def verify(result):
                self.assertEquals(result,None) 
         self.setUpEventCase(distinctId=distinct_id,accountId=account_id,eventName=EventName,properties={})
         self.setUpUserPropertyCase(accountId=account_id,properties={});
         self.sdkTest.addVerify(verify)
         self.sdkTest.run()

    def test_empty_eventId(self):
        self.sdkTest.reset();
        def empty_verify(result):
                self.assertIsNotNone(result)
                self.assertEquals(result.args[0],"event_id must be set")
        def track_update_no_eventid():
            try:
                self.sdk.track_update(account_id=account_id,event_name=EventName)
            except Exception as e:
                return e
        def track_overwrite_no_eventid():
            try:
                self.sdk.track_overwrite(account_id=account_id,event_name=EventName)
            except Exception as e:
                return e

        self.sdkTest.addCase(track_update_no_eventid)
        self.sdkTest.addCase(track_overwrite_no_eventid)
        self.sdkTest.addVerify(empty_verify)
        self.sdkTest.run()
    
    def test_empty_first_check_id(self):
        self.sdkTest.reset();
        def empty_verify(result):
            self.assertIsNotNone(result)
            self.assertEquals(result.args[0],"first_check_id must be set")
        def track_first_no_eventid():
            try:
                self.sdk.track_first(account_id=account_id,event_name=EventName)
            except Exception as e:
                return e
        self.sdkTest.addCase(track_first_no_eventid)
        self.sdkTest.addVerify(empty_verify)
        self.sdkTest.run()
    
    #测试type是否正确
    def test_type(self):
        self.sdkTest.reset();
        def verify(result):
            global msg
            self.assertEquals(result,msg['#type']) 
        self.setUpEventCase(distinctId=distinct_id,accountId=account_id,eventName=EventName,properties={},result=['track','track_update','track_overwrite','track'])
        self.setUpUserPropertyCase(accountId=account_id,properties={},result=['user_set','user_unset','user_setOnce','user_append','user_uniq_append','user_del','user_add']);
        self.sdkTest.addVerify(verify)
        self.sdkTest.run()
        pass

    #测试时间属性处理
    def test_time(self):
        self.sdkTest.reset()
        eventTime = datetime.datetime.utcnow()
        formattime = eventTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        def verify(result):
            global msg
            self.assertEquals(formattime,msg['#time']) 
            self.assertEquals(formattime,msg['properties']['time'])
            self.assertEquals(formattime,msg['properties']['timeobj']['time'])
            self.assertEquals(formattime,msg['properties']['timelist'][0])
        self.setUpEventCase(distinctId=distinct_id,accountId=account_id,eventName=EventName,properties={'#time':eventTime,'time':eventTime,'timeobj':{'time':eventTime},'timelist':[eventTime]})
        
        def user_verify(result):
            global msg
            if result == 'user_set' or result == 'user_setOnce' or result == 'user_append' or result == 'user_uniq_append':
              self.assertEqual(formattime,msg['#time']) 
              self.assertEqual(formattime,msg['properties']['time'])
              self.assertEqual(formattime,msg['properties']['timeobj']['time'])
              self.assertEqual(formattime,msg['properties']['timelist'][0])
            if result == 'user_add':
               self.assertEqual(formattime,msg['#time'])
            
        self.sdkTest.reset()
        self.setUpUserPropertyCase(accountId=account_id,properties={'#time':eventTime,'time':eventTime,'timeobj':{'time':eventTime},'timelist':[eventTime]},result=['user_set','user_unset','user_setOnce','user_append','user_uniq_append','user_del','user_add']);
        self.sdkTest.addVerify(user_verify)
        self.sdkTest.run()

    #测试预制属性处理
    def test_preset_properties(self):
        self.sdkTest.reset()
        eventTime = datetime.datetime.utcnow()
        formattime = eventTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        add_properties = {"#ip":'10.10.10.10',"#app_id":'app_id','#time':eventTime,'#uuid':'00010203-0405-0607-0809-0a0b0c0d0e0f'}
        def verify(result):
            global msg
            if result != 'user_del' and result != 'user_unset':
              self.assertEqual('10.10.10.10',msg['#ip']) 
              self.assertEqual('app_id',msg['#app_id']) 
              self.assertEqual('00010203-0405-0607-0809-0a0b0c0d0e0f',msg['#uuid']) 
              self.assertEqual(formattime,msg['#time'])

        self.setUpEventCase(distinctId=distinct_id,accountId=account_id,eventName=EventName,properties=add_properties)
        self.setUpUserPropertyCase(accountId=account_id,properties=add_properties,result=['user_set','user_unset','user_setOnce','user_append','user_uniq_append','user_del','user_add']);
        self.sdkTest.addVerify(verify)
        self.sdkTest.run()

    def test_user_add(self):
        self.sdkTest.reset()
        def verify(result):
            self.assertIsNotNone(result)
            self.assertEqual(result.args[0],'user_add properties must be number type')
        def user_add():
            try:
                self.sdk.user_add(account_id=account_id,properties={'a':'XXX'})
            except Exception as e:
                return e
        self.sdkTest.addCase(user_add)
        self.sdkTest.addVerify(verify)
        self.sdkTest.run()

    def tearDown(self):
        pass
   

if __name__ == '__main__':
    unittest.main()
