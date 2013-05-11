# -*- coding: utf-8 -*-
import pickle
import gearman
from copy import deepcopy

from src.libs.fast.core import getMD5Hash
from src.libs.fast.fasttwisted import FastJsonServerResource, FastJsonMemcacheServerResource, FastJsonServerResourceDeferred
from src.libs.fast.fastgearman import PickleJobClient, get_time_now, check_request_status

#==============================================================================   
class FastStartGearmanJsonServerResourceDeferred(FastJsonServerResourceDeferred):
    allowedMethods = ('POST',)
    
    #--------------------------------------------------------------------------
    def setUpGearmanJob(self, request, function, param_names, type=None):
        '''
        Setup job to gearman server
        '''
        for name in param_names:
            if not name in request.args:
                raise Exception('Missed param [%s]' % name)
                #return self.getMissedParamResponse(request, name)                    

        #self.logger.ExtInfo('POST PARAMS [%s]' % request.args)
        self.checkHtmlMode(request)
        self._server.checkDbConnection()
         
        data = {
            'params': self.getParamsSet(request, param_names)
        }
        
        data['params']['__callback_type'] = type
        data['params']['setup_time'] = get_time_now()
        server = data['params']['server']
        
        #@todo check active nodes
        nodes_list = self._server.getWorkerNodes()
        
        sql  = '''
        INSERT INTO monitord_task_packet 
            (type, server, setup_time, nodes, retry, result, processed) 
        VALUES (%s, %s, NOW(), %s, 0, 0, 0)'''                                                           

        args = (type, server, ', '.join(nodes_list) )                                                                                                                

        self._server._query(sql, args)                                                                                                                          

        packet_id = int(self._server._cursor.lastrowid)
        self._log('New task packet ID=[#%s]' % packet_id)                                                                                                  
        
        new_client = PickleJobClient([self._server.gearman_server])

        data['params']['job_packet_id'] = packet_id 
        data['params']['nodes'] = nodes_list 
         
        data['job'] = {}
        for node_id in nodes_list:
            '''
            node_params = {}
            for key, value in data['params'].iteritems():
                node_params[key] = value
            '''    
            node_params = deepcopy(data['params'])    
            
            node_params['node'] = node_id
            
            self._log('set job for node=[#%s]' % node_id)
            
            #uids[node_id] = getMD5Hash(pickle.dumps(data))
            current_request = new_client.submit_job(function % node_id, node_params, 
                                            #unique=uids[node_id], 
                                            background=True, wait_until_complete=False)
            
            sql  = '''
            INSERT INTO monitord_log 
                (job_packet_id, job_uid, type, server, node, setup_time, 
                start_time, end_time, run_time, result, processed) 
            VALUES 
                (%s, %s, %s, %s, %s, NOW(), 0, 0, 0, '', 0)'''                                                          
            args = (packet_id, current_request.job.unique, type, server, node_id)                                                                                                                  

            self._server._query(sql, args)                                                                                                                          
            #id = int(self._server._cursor.lastrowid)                                                                                                   
            
            
            #if current_request.job.unique != uids[node_id]:
            #    raise Exception('Different uid')

            
            data['job'][node_id] = {
                    'id' : current_request.job.unique,
                    'state' :  current_request.state,
                    'setup_time' : data['params']['setup_time'],
                    'params' : node_params,
                }
        
            self._log('Job [%s]' % check_request_status(current_request))
            
        data['result'] = True
        return data 
        

#==============================================================================   
class FastStartGearmanJsonMemcacheServerResource(FastJsonMemcacheServerResource):
    allowedMethods = ('POST',)
    
    #--------------------------------------------------------------------------
    def setUpGearmanJob(self, request, function, param_names, type=None):
        '''
        Setup job to gearman server
        @todo mix with FastStartGearmanJsonServerResource
        '''
        try:
            for name in param_names:
                if not name in request.args:
                    return self.getMissedParamResponse(request, name)                    

            #self.logger.ExtInfo('POST PARAMS [%s]' % request.args)
            self.checkHtmlMode(request)
             
            data = {
                'params': self.getParamsSet(request, param_names)
            }
            
            data['params']['__callback_type'] = type

            data['params']['setup_time'] = get_time_now()

            new_client = PickleJobClient([self._server.gearman_server])
            
            uid = getMD5Hash(pickle.dumps(data))
            current_request = new_client.submit_job(function, data['params'], 
                                            unique=uid, background=True, wait_until_complete=False)
            
            if current_request.job.unique != uid:
                raise Exception('Different uid')
            
            data['result'] = True
            
            job_data = {
                    'id' : current_request.job.unique,
                    'state' :  current_request.state,
                    'setup_time' : data['params']['setup_time'],
                    'params' : data['params']
                }
            
            data['job'] = job_data
            
            self.initMemcache()
            timeout = self._server.config.getint('main', 'job_delete_timeout')
            self.mc.set('task-%s' % current_request.job.unique, pickle.dumps(job_data), timeout)
            
            self._log('Job [%s]' % check_request_status(current_request))
                
            return self.returnJsonResponse(request, data) 
        
        except Exception, ex:                                                                                                                                   
            return self.exceptionToJson(request, ex)
        
#==============================================================================   
class FastStateGearmanJsonResource(FastJsonMemcacheServerResource):
    '''
    Статус конкретной задачи
    @todo
    '''
    isLeaf = True    
    allowedMethods = ('GET', 'POST')
    
    logging = True

    #--------------------------------------------------------------------------
    def render(self, request):
        try:
            self.checkHtmlMode(request)  
            data = {}
            
            uid = self.getParam(request, 'uid')
            
            self.initMemcache()
            taks_data = pickle.loads(self.mc.get('task-%s' % uid))
            
#            gm_admin_client = gearman.GearmanClient([self._server.gearman_server])
            data['job_data'] = taks_data
            data['params'] = {
                'uid' : uid
            }
                
            return self.returnJsonResponse(request, data) 
        
        except Exception, ex:                                                                                                                                   
            return self.exceptionToJson(request, ex)    
    
#==============================================================================   
class FastStatusGearmanJsonResource(FastJsonServerResource):
    '''
    Статус сервера задач
    '''
    isLeaf = True    
    allowedMethods = ('GET', 'POST')
    
    logging = True

    #--------------------------------------------------------------------------
    def render(self, request):
        '''
        List of rules
        '''
        try:
            self.checkHtmlMode(request)  
            data = {}

            gm_admin_client = gearman.GearmanAdminClient([self._server.gearman_server])
            
            status_response = gm_admin_client.get_status()
            version_response = gm_admin_client.get_version()
            workers_response = gm_admin_client.get_workers()           
            
            data['result'] = {
                'status' : str(status_response),
                'version' : str(version_response),
                'workers' : str(workers_response)           
            }
                
            return self.returnJsonResponse(request, data) 
        
        except Exception, ex:                                                                                                                                   
            return self.exceptionToJson(request, ex)    
    