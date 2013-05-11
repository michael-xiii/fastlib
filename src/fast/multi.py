# -*- coding: utf-8 -*-
import os
from signal import SIGTERM

from core import FastConfigObject, FastDbObject

    
    
#==============================================================================    
class WorkerMultiprocessServer(FastConfigObject):
    config_file  = 'monitord'
    
    name = 'monitord-worker'
    gearman_server = None

    logging      = True    
    
    #--------------------------------------------------------------------------
    def __init__(self):
        FastConfigObject.__init__(self)
        self.loadConfig()
    
    #--------------------------------------------------------------------------
    def start(self):
        raise Exception('start method must be redefined')

    #--------------------------------------------------------------------------
    def stop(self):
        '''
        Останавливаем всех
        '''
        self.kill_all_workers(os.getpid())   
        
    #--------------------------------------------------------------------------
    def kill_all_workers(self, pid):
        '''
        Убиваем всех работников
        '''
        if not pid:
            self._log('Nothing to kill')
            return
        self._log('Start to shutdown workers')
        pgid = os.getpgid(int(pid))
        self._log('Pid=[%s] Group PID =[%s]' % (pid, pgid))
        
        # мега волшебная строчка
        os.killpg(pgid, SIGTERM)
        
    #--------------------------------------------------------------------------
    def loadConfig(self):
        '''
        Load config - using for 'reload' option from daemon
        '''
        try:
            new_config = self._validatedConfig()
            self._log('Apply config...')
            self.config = new_config
            self.gearman_server = self.config.get('main', 'gearman_server') 

        except Exception, ex:
            self._error('Unable to load config: [%s]' % ex)
            if self.config is None:
                raise Exception('Unable to start - missed config [%s]' % ex)
            else:
                self._error('Unable to validate config - using old one [%s]' % ex)            
            raise
        
#==============================================================================    
class WorkerMultiprocessDbServer(FastDbObject, WorkerMultiprocessServer):
    config_file  = 'monitord'
    name = 'monitord-worker'

    logging      = True    
    
    #--------------------------------------------------------------------------
    def __init__(self):
        FastDbObject.__init__(self)
        WorkerMultiprocessServer.__init__(self)
        self.loadConfig()
        