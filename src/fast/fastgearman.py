#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
sys.path.append("/usr/share/pyshared")

import gearman
import pickle
import memcache
import os

from datetime import datetime, timedelta 
from time import strptime, mktime

from src.libs.fast.core import logger, FastObject, FastDbObject, FastConfigObject, getMD5Hash

#TIME_FORMAT = '%a, %d %b %Y %H:%M:%S +0000'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'


#==============================================================================   
class PickleDataEncoder(gearman.DataEncoder):
    @classmethod
    def encode(cls, encodable_object):
        return pickle.dumps(encodable_object)

    @classmethod
    def decode(cls, decodable_string):
        return pickle.loads(decodable_string)
#==============================================================================   
class PickleJobClient(gearman.GearmanClient):
    '''
    Передаем данные в процесс Worker через Gearman
    '''
    data_encoder = PickleDataEncoder


#==============================================================================   
def check_request_status(job_request):
    '''
    Проверка статуса
    '''
    if job_request.complete:
        return "Job %s finished!  Result: %s - %s" % (job_request.job.unique, job_request.state, job_request.result)
    elif job_request.timed_out:
        return "Job %s timed out!" % job_request.unique
    elif job_request.state == gearman.JOB_UNKNOWN:
        return "Job %s connection failed!" % job_request.unique
    
    return None



#==============================================================================   
class CustomGearmanWorker(gearman.GearmanWorker):
    def on_job_execute(self, current_job):
        logger.Info('Job [%s] started' % current_job.unique)
        return super(CustomGearmanWorker, self).on_job_execute(current_job)

    def on_job_exception(self, current_job, exc_info):
        #params = PickleDataEncoder.decode(current_job.data)
        
        logger.Error('Job [%s] failed, CAN stop last gasp GEARMAN_COMMAND_WORK_FAIL [%s]' % (current_job.unique, exc_info))
        return super(CustomGearmanWorker, self).on_job_exception(current_job, exc_info)

    def on_job_complete(self, current_job, job_result):
        logger.Info('Job [%s] successfully complete with [%s] result' % (current_job.unique, job_result))
        return super(CustomGearmanWorker, self).send_job_complete(current_job, job_result)

    def after_poll(self, any_activity):
        # Return True if you want to continue polling, replaces callback_fxn
        return True
    
    
#==============================================================================
def child(gearman_server, func):
    '''
    Порождаем процесс-потомок
    '''
    logger.Info('A new child [%s]' %  os.getpid( ))
    func(gearman_server)
    os._exit(0)
    
#==============================================================================
def get_time_now():
    return datetime.now().strftime(TIME_FORMAT)      



    
    
#==============================================================================
class WorkerProcessor(FastObject):
    logging = True
    #--------------------------------------------------------------------------
    def __init__(self, config):
        FastObject.__init__(self)
        self.config = config

    #--------------------------------------------------------------------------
    def _log(self, msg):
        msg = 'PID: [%s] %s' % (os.getpid( ), msg)
        FastObject._log(self, msg)  

    #--------------------------------------------------------------------------
    def _error(self, msg):
        msg = 'PID: [%s] %s' % (os.getpid( ),  msg)
        FastObject._error(self, msg)
        
    #--------------------------------------------------------------------------
    def getRunningTime(self, start_time):
        '''
        Get running time
        '''
        data = {}
        data['end_time'] = get_time_now()
        end_time = mktime(strptime(data['end_time'], TIME_FORMAT))
        #self._log('end_time %s' % task_data['end_time'])

        data['start_time'] = start_time
        start_time = mktime(strptime(data['start_time'], TIME_FORMAT))
        #self._log('start_time %s' % start_time)

        run_time = timedelta(seconds=end_time-start_time)
        #self._log('run_time %s' % str(run_time))
        
        #@convert to sec        
        data['run_time'] = str(run_time)
        return data
        
        


#==============================================================================
class WorkerMemcacheProcessor(WorkerProcessor):
    mc = None
    lock_expire = None
    lock_key = 'locked-%s'
    
    #--------------------------------------------------------------------------
    def __init__(self, config):
        WorkerProcessor.__init__(self, config)
        self.lock_expire = self.config.getint('memcache', 'lock_expire') * 60
        self.gearman_server = self.config.get('main', 'gearman_server') 
        
    #--------------------------------------------------------------------------    
    def setLock(self, ip):
        self.mc.set(self.lock_key % ip, 'locked', self.lock_expire)

    #--------------------------------------------------------------------------    
    def isLocked(self, ip):
        value = self.mc.get(self.lock_key % ip)
        if value == 'locked':
            return True

        return False

    #--------------------------------------------------------------------------    
    def removeLock(self, ip):
        self.mc.delete(self.lock_key % ip)
        
    
    #--------------------------------------------------------------------------
    def initMemcache(self):
        '''
        Reconnect to memcache
        '''
        memserver = self.config.get('memcache', 'server')
        
        self._log('Try connect to memcache [%s]' % memserver)
        self.mc = memcache.Client( [ memserver ], debug=1)

        if not self.mc:
            raise Exception('Unable to connect to memcache at [%s]' % memserver)
        
        self._log('Enabling Memcache [%s]' % self.mc)
        
        
    #--------------------------------------------------------------------------
    def updateJob(self, uid, start_time, data=None):
        '''
        обновляем информацию о задаче
        '''
        self.initMemcache()
        #self._log('Update job')
        
        task_key = 'task-%s' % uid
        
        task_data = pickle.loads(self.mc.get(task_key))
        self._log('old data [%s]' % task_data)
        
        
        time_data = self.getRunningTime(start_time)
        
        for key, value in time_data:
            task_data[key] = value
        
        self.mc.replace(task_key, pickle.dumps(task_data))
        self._log('Update job=[%s] data=[%s]' % (uid, task_data)) 

    #--------------------------------------------------------------------------
    def run(self, params):
        raise Exception('Run method must redefined')   
    


#==============================================================================
class WorkerDbProcessor(WorkerProcessor, FastDbObject):
    logging = True
    
    #--------------------------------------------------------------------------
    def __init__(self, config):
        WorkerProcessor.__init__(self, config)
        FastDbObject.__init__(self)
        
    #--------------------------------------------------------------------------
    def updateJob(self, uid, start_time, data=None):
        '''
        обновляем информацию о задаче
        '''
        self.checkDbConnection()
        
        self._query('SELECT * FROM monitord_log WHERE uid=%s' % uid)
        rows = self._cursor.fetchall()
        
        if len(rows) != 1:
            raise Exception('To many records with job_uid=[%s]' % uid)
        
        end_time = mktime(strptime(get_time_now(), TIME_FORMAT))
        start_time = mktime(strptime(get_time_now(), TIME_FORMAT))

        run_time = timedelta(seconds=end_time-start_time)
        
        self._log('start_time=[%s], end_time=[%s], run_time=[%s])' %  (start_time, end_time, run_time))
        
        self._query('UPDATE monitord_log SET (start_time, end_time, run_time) VALUES (%s, %s, %s) WHERE uid=%s' % (start_time, end_time, run_time, uid))

    #--------------------------------------------------------------------------
    def run(self, params):
        raise Exception('Run method must redefined')   


        
