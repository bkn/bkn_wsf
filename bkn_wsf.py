#!/usr/bin/env python
# coding: utf-8

## \mainpage 
# 
# bkn_wsf.py includes wrappers and examples for using structWSF, a framework for interacting with 
# an RDF repository. For more information about structWSF see, http://openstructs.org/structwsf
#
# structwsf supports a variety of data formats (RDF, XML, JSON) and converts the data to and from RDF.
# bkn_wsf is tailored to use JSON data with structwsf, and in particular a format structwsf calls 
# irJSON. This irJSON format is what you typically think of when you work with JSON data. Some 
# structwsf responses are in a JSON structure that would be more familiar to someone working with RDF.
# BKN projects usually refered to the irJSON format as bibJSON.
#
# bkn_wsf.py simplifies use of structwsf and handles conversions required to use 
# structwsf with irJSON (bibJSON). BKN projects use bkn_wsf to import large datasets into structwsf
# instances/repositories. bkn_wsf can also be used as a proxy web services for use by javascript apps 
# (See method BKNWSF.web_proxy_services). 
#  
# Every structwsf instance used by BKN has a corresponding Drupal site with a module called conStruct
# that integrates structwsf with drupal. The drupal site is used for various management 
# tasks, and can be used to search and browse data. http://people.bibkn.org is the first structwsf 
# instance created for BKN. There are now over one million records in the repository. BKN does 
# not make use of most Drupal/conStruct functionality which includes the ability to skin display of 
# different types of data. 
#  
# structwsf authentication is handled by associating an ip address with a login for the drupal site 
# associated with the structwsf repository. Most BKN datasets are have public read-only permissions. 
# Login is required for write access to a dataset, and to read private datasets. For example,
# to write to datasets on people.bibkn.org 
#    
#    REGISTER FOR AN ACCOUNT: http://people.bibkn.org/drupal/user/register
#    LOGIN:                   http://people.bibkn.org/user
#
#    Then 'JOIN' the dataset: http://people.bibkn.org/og/
#
#
# EXAMPLES: 
#
# The quickest way to learn bkn_wsf functionality is to review a method in the Test class at the 
# bottom of this source file, Test.autotest()
#
# For questions and comments contact info@bibkn.org.
#
#
# SOURCE CODE:
#
# bkn_wsf is just a single file which can be downloaded from, 
#
# http://github.com/tigerlight/bkn_wsf
#
# To report bugs or review proposed features see http://github.com/tigerlight/bkn_wsf/issues
#
#
# This subject material is based upon work supported by the National Science Foundation
# to the Bibliographic Knowledge Network (http://www.bibkn.org) project 
# under NSF Grant Award No. 0835851.
#
#
# CHANGES since last check-in:
# 8/19/2010
#
# new class Test
#    moved test functions into class
#    
# BKNWSF
#    moved the following functions into class with new names: 
#        BKNWSF.structwsf_request, 
#        BKNWSF.structwsf_request_curl, 
#        web_proxy_services(bkn_wsf_py_services) and related functions
#        convert_???????
#
# Started documentation in a form that can be used by doxygen to generate an HTML manual
#

import re, os, logging
from logging import handlers
from os import getenv,path
from datetime import *
import time
import codecs
import urllib
import urllib2
import urlparse
#from urlparse import parse_qs
import simplejson
import sys
import cgi, cgitb 

#print os.getcwd()

## Configure debug logging
#
class Logger():
    ##Setting up logging
    def __init__(self):
        self.formatter = logging.Formatter("%(asctime)s - %(name)s\
         - %(levelname)s - %(message)s")
        self.log = logging.getLogger()
        self.log.setLevel(logging.DEBUG)
        self.setFileHandler()
        #disable next line to silence stdout
        self.setStreamHandler()
    def debug(self, message):
         self.log.debug(message)
    
    ## Set the log file
    def setFileHandler(self, filename = 'log.txt'):
        base_path = os.path.abspath("")
        log_filepath = os.path.join(base_path, "log.txt")
        #handler writes to 5 files, creating a new one
        #when one gets too large, of the form
        #log.txt, log.txt1, log.txt2...
        handler = logging.handlers.RotatingFileHandler(\
            log_filepath, maxBytes=1048576, backupCount=5)
        handler.setFormatter(self.formatter)
        self.log.addHandler(handler)

    ##StreamHandler writes to stdout
    def setStreamHandler(self):
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(self.formatter)
        self.log.addHandler(ch)

global logger
logger = Logger()



def microtime_id(): 
    d = datetime.utcnow()
    return d.strftime('%Y%m%d%H%M%S')+str(d).split('.')[1]

def slash_end (s):
    #Appends a trailing '/' if a string doesn't have one already
    if (s[-1] != '/'): s += '/'
    return s

def unslash_end (s):    
    #Removes the trailing '/' if a string has one
    return s.rstrip('/')


## BKNWSF is manage the structwsf instance root uri, and includes methods for operations
# which are not specific to a dataset. 
class BKNWSF:
    part = {
        'root': '',
        'ip': os.getenv("REMOTE_ADDR")
            }

    if ((not part['ip']) or (part['ip'] == '::1')): 
        # This means we are executing locally
        # We need to get the external ip address for the local machine
        bkn_wsf = 'http://services.bibsoup.org/cgi-bin/structwsf/bkn_wsf.py'
        part['ip'] = str(urllib2.urlopen(bkn_wsf,'&service=get_remote_ip').read()).strip()
        #urllib.urlopen('http://www.whatismyip.com/automation/n09230945.asp').read()

    ##
    @staticmethod
    def set (value, k):
        if (k == 'root'):
            BKNWSF.part[k] = slash_end(value)
        return BKNWSF.get()
    ##
    @staticmethod
    def get(k='root'):
        response = ''
        if (k == 'root'):
            response = BKNWSF.part['root'] 
        return response
    ##
    @staticmethod
    def ip(format=''):
        if (format == 'param'):
            response = ''
            if (BKNWSF.part['ip']):
                response = '&registered_ip='+BKNWSF.part['ip']
        else:
            response = BKNWSF.part['ip']
        return response
    ## 
    @staticmethod
    def structwsf_request (service, params, http_method="post", accept_header="application/json", deb = 0):
        deb = 0
        if (service[-1] != '/'): service += '/' 
        # as of 6/8/10 the service root to call services uses /ws/
        # and the service root when referring to a service is /wsf/ws/
        #s = 'http://people.bibkn.org/ws/'+ service
        s = str(Service.get('root') + service).strip()
        p = str(params + BKNWSF.ip('param')).strip()
        response_format = "json"
        header = {"Accept": accept_header.strip()}
        if ((accept_header == 'bibjson') or (accept_header == 'application/iron+json')):
            header['Accept'] = "application/iron+json"
        elif (accept_header == "json"):
            header['Accept'] = "application/json"    
        else:
            response_format = "other"
    
    # This output is helpful to include when reporting bugs
        if deb: debug( '\n\nHEADER:\n'+str(header))
        if deb: debug( '\nREQUEST: \n'+s+'?'+p)
        response = None
        #print s+"?"+p
        #print header
        
        delta = time.time()
        try:
            if (http_method == "get"):
                req = urllib2.Request(s+"?"+p, headers = header)        
            else: # use post
                req = urllib2.Request(s, headers = header, data = p)        
            fp = urllib2.urlopen(req)        
            '''
            TODO: check fp.code status
            '''
        except urllib2.HTTPError, err: 
            response = {'error':'HTTPError','reason':err.code, 'urllib2':str(err)}
        except urllib2.URLError, err: 
            response = {'error':'URLError','reason':err.reason}
        except:
            response = {'error':'unknown','reason':''}
        else:
            response = fp.read()
            fp.close()
            
        delta = time.time() - delta
        if deb: debug( '\nWSF CALL RESPONSE: (response time:'+str(delta)+')\n'+ str(response))
        try:
            if (response and (not isinstance(response, dict)) and (response_format == "json")):
                response = simplejson.loads(response)
        except: # this catches url and http errors        
            if deb: debug( 'BAD JSON:')
            if (not isinstance(response, dict)): debug( response.replace('\\n','\n'))
            response = {'error':'simplejsonError','reason':'bad json', "response":response}
        
        #print '\nWSF CALL RESPONSE:\n', response        
        return response
    
    ##
    @staticmethod
    def structwsf_request_curl (service, params, http_method="post", accept_header="application/json", deb = 0):
        deb = 0
        if (service[-1] != '/'): service += '/' 
        # as of 6/8/10 the service root to call services uses /ws/
        # and the service root when referring to a service is /wsf/ws/
        #s = 'http://people.bibkn.org/ws/'+ service
        s = Service.get('root') + service
        p = params + BKNWSF.ip('param')
        response_format = "json"
        header = {"Accept": accept_header}
        if ((accept_header == 'bibjson') or (accept_header == 'application/iron+json')):
            header['Accept'] = "application/iron+json"
        elif (accept_header == "json"):
            header['Accept'] = "application/json"    
        else:
            response_format = "other"
    # This output is helpful to include when reporting bugs
        if deb: logger.debug( '\n\nHEADER:\n'+str(header))
        if deb: logger.debug( '\nREQUEST: \n'+s+'?'+p)
        response = None
        #print s+"?"+p
        #print header
        
        headerstring = ""
        for key in header:
            headerstring+='-H "%s : %s" ' %(key, header[key])
    
        try:
            if (http_method == "get"):
                command = 'curl '+headerstring+'"%s" "%s"' %(s+"?"+p)
                logger.debug(command)
                logger.debug( os.system(command))
                req = urllib2.Request(s+"?"+p, headers = header)        
            else: # use post
                writing = open("params.txt","w")
                writing.write(p)
                writing.close()
                command = 'curl '+headerstring+'-d @%s "%s"' %('params.txt', s)
                logger.debug( command)
                logger.debug( os.system(command))
                req = urllib2.Request(s, headers = header, data = p)        
            fp = urllib2.urlopen(req)        
            '''
            TODO: check fp.code status
            '''
        except urllib2.HTTPError, err: 
            response = {'error':'HTTPError','reason':err.code, 'urllib2':str(err)}
        except urllib2.URLError, err: 
            response = {'error':'URLError','reason':err.reason}
        except:
            response = {'error':'unknown','reason':None}
        else:
            response = fp.read()
            fp.close()
            
        if deb: logger.debug( '\nWSF CALL RESPONSE:\n'+ str(response))
        try:
            if (response and (not isinstance(response, dict)) and (response_format == "json")):
                response = simplejson.loads(response)
        except: # this catches url and http errors        
            if deb: logger.debug( 'BAD JSON:')
            if (not isinstance(response, dict)): logger.debug( response.replace('\\n','\n'))
            response = {'error':'simplejsonError','reason':'bad json', "response":response}
        
        #print '\nWSF CALL RESPONSE:\n', response        
        return response
    
    
    
    ##
    @staticmethod
    def convert_bibtex_to_text_xml(data):
        mime = "&docmime="+urllib.quote_plus("application/x-bibtex")
        doc = "&document="+urllib.quote_plus(data)
        params = mime + doc
        response = BKNWSF.structwsf_request("converter/bibtex/", params, "post","text/xml")
        return response     
    
    @staticmethod
    def convert_text_xml_to_json(data):
        mime = "&docmime="+urllib.quote_plus("text/xml")
        doc = "&document="+urllib.quote_plus(data)
        params = mime + doc
        response = BKNWSF.structwsf_request("converter/irjson/", params, "post",'bibjson')    
        if (not isinstance(response,dict)): # only an error would return dict
            response = {'error':response}
        return response     
    
    @staticmethod
    def convert_json_to_text_xml(data):
        mime = "&docmime="+urllib.quote_plus("application/iron+json")
        doc = "&document="+urllib.quote_plus(simplejson.dumps(data))
        params = mime + doc
        response = BKNWSF.structwsf_request("converter/irjson/", params, "post","text/xml")
        return response     
    
    @staticmethod
    def convert_json_to_rdf(data):
        mime = "&docmime="+urllib.quote_plus("application/iron+json")
        doc = "&document="+urllib.quote_plus(simplejson.dumps(data))
        params = mime + doc
        response = BKNWSF.structwsf_request("converter/irjson/", params, "post","application/rdf+xml")
        return response    
    
    @staticmethod
    def get_web_proxy_service_param(cgi_fields, key):
        response = None
        params = cgi_fields.getfirst('params')
        obj = None
        if (params): 
            obj = cgi.parse_qs(params) # > py2.5 use urlparse or urllib.parse            
        if (obj and
            obj.has_key(key) and 
            obj[key] and 
            isinstance(obj[key],list) and
            len(obj[key])
            ):
            response = obj[key][0]
        
        return response  
    
    
    ##
    @staticmethod
    def web_proxy_services (cgi_fields):
        '''
        TO TEST SET, if 1:
        '''
        if 0:
            bkn_root = 'http://datasets.bibsoup.org/wsf/'
            #bkn_root = 'http://people.bibkn.org/wsf/'
            service_root = bkn_root+'ws/'
            dataset_root = bkn_root+'datasets/'            
            service =        'dataset_list'
            query = 'pitman'
            items = ''
            page =  ''
            dataset_uri =       dataset_root+'new_jack/'
            datasets_uri_list = 'all'
            record_uri =        dataset_uri+'jack3'
            bibjson =           ''
    
        callback = ''
        if (cgi_fields and ('callback' in cgi_fields)): 
            callback = cgi_fields.getfirst('callback') 
        
        if ((not callback) and cgi_fields and ('service' in cgi_fields) and 
                (cgi_fields.getfirst('service') == 'get_remote_ip')):
                #don't semd end of line for print. We want to return a single line string      
                print 'Content-type: text/plain \n\n',
                print str(BKNWSF.ip()),
                return    
        else:       
            if (cgi_fields):
                bkn_root =          cgi_fields.getfirst('bkn_root')           
                service_root =      cgi_fields.getfirst('service_root')
                dataset_root =      cgi_fields.getfirst('dataset_root')            
                service =           cgi_fields.getfirst('service')
                query =             BKNWSF.get_web_proxy_service_param(cgi_fields,'query')
                items =             BKNWSF.get_web_proxy_service_param(cgi_fields,'items')
                page =              BKNWSF.get_web_proxy_service_param(cgi_fields,'page')
                datasets_uri_list = str(BKNWSF.get_web_proxy_service_param(cgi_fields,'datasets'))
                dataset_uri =       str(BKNWSF.get_web_proxy_service_param(cgi_fields,'dataset'))
                record_uri =        str(BKNWSF.get_web_proxy_service_param(cgi_fields,'uri'))
                bibjson =           BKNWSF.get_web_proxy_service_param(cgi_fields,'document')
        
            response = {}
            error = False
            BKNWSF.set(bkn_root,'root') # 'http://people.bibkn.org/wsf/'
            if(not bkn_root or (bkn_root == '')):
                error = True
                response['error'] = 'bkn_root was not specified.'
        
            Service.set(BKNWSF.get()+'ws/','root')
            if (service_root): Service.set(service_root,'root')
            Dataset.set(BKNWSF.get()+'datasets/','root')            
            if (dataset_root): Dataset.set(dataset_root,'root')
        
            if (bibjson):               bibjson = simplejson.loads(bibjson)
            if (dataset_uri):           Dataset.set(dataset_uri, 'uri')
            if (record_uri):            Record.set(record_uri, 'uri')
            if (not datasets_uri_list): datasets_uri_list = dataset_uri
            if (not page):              page = '0'
            if (not items):             items = '10'
            
            if (error):
                if (not response): 
                    response = {'error': 'unknown'}        
            elif (not service):
                response['error'] = 'No service specified'
                error = True
            elif service == 'test':
                Dataset.set('jack_update_test')
                response = {} # need to make sure to return a dict not ""
                error = True             
            elif service == 'search':
                '''
                ADD FORMATTED FACETS TO RETURN
                ''' 
                response = BKNWSF.search(query, datasets_uri_list, items, page)
                if (not response): 
                    response = {} # need to make sure to return a dict not ""
                    error = True
            elif service == 'dataset_list':
                response = Dataset.list()
                if (not response): 
                    response = {} # need to make sure to return a dict not ""
                    error = True        
            elif service == 'dataset_create':
                #print simplejson.dumps(response,indent=1)
                #(ds_id='', title='', description='', creator='')
                record_uri = '' # by default this uses the uri parameter, so clear it in case it gets used
                dataset_uri =   str(BKNWSF.get_web_proxy_service_param(cgi_fields,'uri'))
                title =         str(BKNWSF.get_web_proxy_service_param(cgi_fields,'title'))
                description =   str(BKNWSF.get_web_proxy_service_param(cgi_fields,'description'))
                Dataset.set(dataset_uri)
                response = Dataset.create(dataset_uri, title, description)
                if (response): 
                    error = True
                else:
                    response = {} # need to make sure to return a dict not ""
            elif service == 'dataset_delete':
                '''
                Need to test, may not ever enable this as a service
                '''
                #print simplejson.dumps(response,indent=1)
                #response = Dataset.delete()
                if (response): 
                    error = True
                else:
                    response = {} # need to make sure to return a dict not ""
            elif service == 'browse':
                '''
                ADD FORMATTED FACETS TO RETURN
                '''             
                response = BKNWSF.browse(datasets_uri_list, items, page)
                if (not response): 
                    response = {} # need to make sure to return a dict not ""
                    error = True
            elif service == 'record_read':
                response = Record.read(Record.set(record_uri))
                if (not response): 
                    response = {} # need to make sure to return a dict not ""
                    error = True
            elif service == 'record_update': 
                response = Record.update(bibjson)
                if (not response): 
                    response = Record.read(Record.set(record_uri))
                    if (not response): 
                        response = {} # need to make sure to return a dict not ""
                        error = True
                else:
                    error = True
            elif service == 'record_add':   
                # This can be used to add or update a single attribute
                response = Record.add(bibjson)
                if (not response): 
                    response = Record.read(Record.set(record_uri))
                    if (not response): 
                        response = {} # need to make sure to return a dict not ""
                        error = True
                else:
                    error = True
            elif service == 'record_delete':   
                response = Record.delete(Record.set(record_uri), Dataset.set(dataset_uri, 'uri'))
                if (response): 
                    error = True
                else:
                    response = {} # need to make sure to return a dict not ""
                    #response = Record.read()
                
            #response = simplejson.dumps(bigd)#.replace('\n','<br>').replace('  ','&nbsp;&nbsp;')
            #if 'jsonp' in cgi_fields: callback  = cgi_fields.getfirst('jsonp') 
            #response = browse(ds_id, 10, 0, '')      
            if error:
                response['params'] = '' #cgi_fields.getfirst('params')
        
            print 'Content-type: text/plain \n\n'
            print callback+'('+simplejson.dumps(response)+')'

    ##
    @staticmethod
    def search(query, ds_uris='', items=10, page=0, other_params='&include_aggregates=true'):
        if not ds_uris: ds_uris = Dataset.get('uri')
        params = '&query=' + query
        # ds_uris can accept multiple uris so we don't want to update the Dataset object
        # Default to curremt datast if none is passed
        # This means 'all' needs to be explicitly passed to search all datasets
        params += '&datasets=' + ds_uris
        if (items): params += '&items='+str(items)
        if (page):  params += '&page='+str(page)
        # other params: &types= &attributes= &inference= &include_aggregates=
        params += other_params
        response = BKNWSF.structwsf_request('search', params, 'post', 'text/xml')
        data = Dataset.get('detail',response)
        return data

    ##
    @staticmethod
    def browse(ds_uris='', items='10', page='0', other_params='&include_aggregates=True'):
        # This is kind of an export.
        params = ''
        # ds_uris can accept multiple uris so we don't want to update the Dataset object
        # Default to curremt datast if none is passed
        # This means 'all' needs to be explicitly passed to search all datasets
        if (not ds_uris): 
            ds_uris = Dataset.get('uri')
        params += '&datasets=' + ds_uris
        if (items): params += '&items='+str(items)
        if (page):  params += '&page='+str(page)
        # other params: attributes= &types= &inference=
        params += other_params
        response = BKNWSF.structwsf_request("browse", params, 'post', 'text/xml')
        data = Dataset.get('detail',response)
        return data    

##
class Service:
    part = {
        'root': ''
            }
    ##
    @staticmethod
    def set (value, k):
        if (k == 'root'):
            Service.part[k] = slash_end(value)
        return Service.get()
    ##
    @staticmethod
    def get(v='root'):
        return Service.part[v]
    
    
##
class Dataset:
    '''
    CHECK FOR VALID PATHS - NO SPACES OR WEIRD CHARS
    '''    
    part = {
        'root': '',
        'uri': '',
        'id': '',
        'template': {
            #"type":"dataset",
            "id": "",
            "schema": [
                "identifiers.json",
                "type_hints.json",
                "http://downloads.bibsoup.org/datasets/bibjson/bibjson_schema.json",
                "http://www.bibkn.org/drupal/bibjson/bibjson_schema.json"
                ],                
            "linkage": ["http://www.bibkn.org/drupal/bibjson/iron_linkage.json"]
            }
        }
    @staticmethod
    def make_root(uri):
        # guess that the root is the string after the last non-trailing slash
        root_end = Dataset.part['uri'].rfind('/',0,-1) + 1
        Dataset.part['root'] = slash_end(Dataset.part['uri'][0:root_end])
        return Dataset.part['root']         

    @staticmethod
    def extract_dataset_uri_from_browse_response(r):
        uri = ''
        if ('isPartOf' in r):
            if ('ref' in r['isPartOf']):
                uri = r['isPartOf']['ref'].replace('@@','')
        return uri
            
    ##
    @staticmethod
    def set (value, k=''):
        '''
        NOTE: When setting uri or id, the root will not be updated unless it is None
        '''
        # Allow call with value = None, return current uri but don't do anything else
        # 
        if (value and (not k)) : # value is either a uri or id
            if ((value[0:2] == '@@') or (value[0:7] == 'http://')): # value is a uri
                Dataset.set(value, 'uri')
            else: # value is an id
                Dataset.set(value,'id')            
        elif (value and (k in Dataset.part)):
            if(k == 'id'):
                if (not Dataset.part['root']):
                    Dataset.make_root(Dataset.part['uri'])
                Dataset.part['id'] =  unslash_end(value.replace('@','',2))
                Dataset.part['uri'] =  slash_end(Dataset.part['root'] + Dataset.part['id'])                
                #Dataset.part['uri'] = Dataset.part['id']
            elif (k == 'uri'):
                Dataset.part['uri'] =  slash_end(value.replace('@','',2))
                #Dataset.part['id'] = Dataset.part['uri']
                if (not Dataset.part['root']):
                    Dataset.make_root(Dataset.part['uri'])
                Dataset.part['id'] = unslash_end(Dataset.part['uri'].replace(Dataset.part['root'],''))
            elif (k == 'root'):
                Dataset.part['root'] = slash_end(value)

        return Dataset.get()            
    
    ##
    @staticmethod
    def get(k='uri', v=''):
        def get_detailed_response(response):
            if (not isinstance(response, dict)): # only an error would return dict    
                response = BKNWSF.convert_text_xml_to_json(response)   
            if ('error' in response):
                data = response
            else:
                data = {'dataset':{},'recordList':[]}
        
            if ('dataset' in response):
                data['dataset'] = response['dataset']
        
            if ('recordList' in response):
                for r in response['recordList']:
                    if (('type' in r) and (r['type'] == 'Aggregate')):
                        if ('aggregate' not in data):
                            data['aggregate'] = []
                        data['aggregate'].append(r)
                    elif (('id' in r) ):
                        Dataset.set(Dataset.extract_dataset_uri_from_browse_response(r), 'uri')
                        record = Record.read(r['id'])
                        if ('recordList' in record):
                            data['recordList'].extend(record['recordList'])
                        elif ('error' in record):
                            data['recordList'].append(record)
            return data
        
        if (k == 'detail'):
            response = get_detailed_response(v)
        else:
            response = Dataset.part[k]
        return response    


    ##
    @staticmethod
    def read(ds_uri='', other_params=''):
        params = '&uri='
        if (ds_uri == 'all'):
            params += 'all'
        else:
            params += Dataset.set(ds_uri, 'uri')
                    
        if (other_params): 
            params += other_params
        else:
            params += '&meta=True' # + '&mode=dataset'
            
        response = BKNWSF.structwsf_request("dataset/read/",params,"get", 'text/xml')
        if (not isinstance(response,dict)):
            response = BKNWSF.convert_text_xml_to_json(response)
        return response

    ##
    @staticmethod
    def delete(ds_id=''):
        params = '&uri=' + urllib.quote_plus(Dataset.set(ds_id))
        response = BKNWSF.structwsf_request("dataset/delete", params, "get") 
        return response
        
    ##
    @staticmethod
    def create(ds_id='', title='', description=''):
        ds = '&uri=' + Dataset.set(ds_id) # urllib.quote_plus(Dataset.set(ds_id))
        # Parameter default for title is not used because ds_id may be passed in
        if (not title): title = Dataset.get('id') 
        params = ds + '&title=' + urllib.quote_plus(title)
        if (description): params += '&description=' + urllib.quote_plus(description)
        response = BKNWSF.structwsf_request("dataset/create", params, "post") 
        if (not response):
            response = Dataset.auth_registrar_access(Dataset.get())     
        return response
    
    ##
    @staticmethod
    def auth_registrar_access(ds_id='', action='create'):
        ds = '&dataset=' + Dataset.set(ds_id)
        params = ds
        params += '&ws_uris='
        wsf_service_root = Service.get()
        services = wsf_service_root+'crud/create/;'
        services += wsf_service_root+'crud/read/;'
        services += wsf_service_root+'crud/update/;'
        services += wsf_service_root+'crud/delete/;'
        services += wsf_service_root+'search/;'
        services += wsf_service_root+'browse/;'
        services += wsf_service_root+'dataset/read/;'
        services += wsf_service_root+'datast/delete/;'
        services += wsf_service_root+'dataset/create/;'
        services += wsf_service_root+'dataset/update/;'
        services += wsf_service_root+'converter/irjson/;'
        services += wsf_service_root+'sparql/'
        permissions = '&crud='+urllib.quote_plus('True;True;True;True')
        params += urllib.quote_plus(services) + permissions
        params += '&action='+action
        response = BKNWSF.structwsf_request('auth/registrar/access', params)
        return response
    
    ##
    @staticmethod
    def ids(other_params=''):
        params = '&mode=dataset'
        if (other_params): params += other_params
        response = BKNWSF.structwsf_request("auth/lister", params, "get", 'text/xml')
        if (isinstance(response,dict)): # only an error would return dict
            response = {'error': response}
        else:
            response = BKNWSF.convert_text_xml_to_json(response)
        return response
    
    ##
    @staticmethod
    def list(v='detail', other_params=''):    
    # it may be possible to avoid multiple calls by using '&uri=all' with Dataset.read
        response = None
        params = '&mode=dataset'
        if (other_params): params += other_params
        ds_ids = BKNWSF.structwsf_request("auth/lister", params, "get", 'text/xml')
        if (isinstance(ds_ids,dict)):
            # a dict means there was an error
            response = ds_ids
        else:
            ds_ids = BKNWSF.convert_text_xml_to_json(ds_ids)
            
            if (v == 'ids'):
                response = ds_ids                
            elif (ds_ids and ('error' not in ds_ids) and ('recordList' in ds_ids)):
                ds_root = Dataset.get('root')
                response = {'recordList':[]}
                for r in ds_ids['recordList']:
                    if ('li' in r):
                        for d in r['li']:
                            if ('ref' in d):
                               ds_uri = d['ref'].replace('@@','')
                               if (ds_root and (ds_root in ds_uri)):
                                   ds = Dataset.read(ds_uri)
                                   if ('dataset' in ds):
                                       response['recordList'].append(ds['dataset'])       
        return response

    ##
    @staticmethod
    def template():
        return Dataset.part['template']
    

##
class Record:
    '''
    CHECK FOR VALID PATHS - NO SPACES OR WEIRD CHARS
    '''
    '''
    RECENTLY LEARNED ID=URI SO MUCH OF THIS CODE CAN BE CLEANED UP
    
    CHECK FOR ID VS. URI BY LOOKING AT @ VERSUS @@
    
    SET ID BY STRIPPING DATASET_URI
    SET URI BY ADDING DATASET_URI
    '''
    part = {
        'uri': '',
        'id': ''   
        }
    ##
    @staticmethod
    def set (value, k=''):        
        if (value and (not k)) : # value should be either a uri or id            
            if ((value[0:2] == '@@') or (value[0:7] == 'http://')): # value is a uri                    
                Record.set(value, 'uri')
            else:
                Record.set(value, 'id')
        elif (value and (k in Record.part)):
            if(k == 'id'):
                Record.part['id']  = unslash_end(value.replace('@','',2))
                Record.part['uri'] = Dataset.get()+Record.part['id'] 
            elif (k == 'uri'):
                Record.part['uri'] = unslash_end(value.replace('@','',2))
                Record.part['id'] = unslash_end(Record.part['uri'].replace(Dataset.get(),'')) 
                #id_start = Record.part['uri'].rfind('/',0) + 1
                #Dataset.set(Record.part['uri'][0:id_start-1]) # dataset not necessarily root of uri
                #Record.part['id'] = Record.part['uri']
                
        return Record.get('uri')

    ##
    @staticmethod
    def get (v='uri', k=''):
        response = Record.part['uri']
        if (k and (k == 'id')):
            response = Record.part['id']            
        return response 

    ##
    @staticmethod
    def read(record_id='', ds_id='', other_params=''):
        params = ''
        #params += '&include_linksback=True&include_reification=True'
        # if an id is none 'set' will return the current uri
        
        params += '&uri=' + Record.set(record_id, 'uri') # do this first so dataset uri gets set
        params += '&dataset=' + Dataset.set(ds_id)
        params += other_params
        response = BKNWSF.structwsf_request('crud/read', params, 'get', 'bibjson')
        return response
    
    ##
    @staticmethod
    def update(record, ds_id=''):
        '''
            record should have an id
            SHOULD SET Record.set() id, need to test for recordList or 'id'
            
            SEEMS LIKE STRUCTWSF PREPENDS DATASET URI TO RECORD ID
            SO THE RECORD ID IN BIBJSON MUST NOT INCLUDE THE DATASET URI
            
        '''
        Dataset.set(ds_id) #Dataset.set(ds_id, 'uri')
        response = None
        rdf = None
        bibjson = {}
        bibjson['recordList'] = []
        bibjson['dataset'] = Dataset.template()
        bibjson['dataset']['id'] = Dataset.get('uri') #Dataset.get('uri')
        '''
        TODO: CHECK record['id] TO MAKE SURE IT LOOKS VALID
        '''
        if (isinstance(record,dict) and ('dataset' in record)):
            # the record is in full bibjson format
            bibjson = record
        elif (isinstance(record,dict)):
            bibjson['recordList'].append(record);                            
        elif (isinstance(record,list)):
            bibjson['recordList'].extend(record)
        else:
            # assume record is a fully-baked dataset already converted to an rdf string
            rdf = record
            
        if (not rdf):
            rdf = BKNWSF.convert_json_to_rdf(bibjson)   

        if(isinstance(rdf,dict) ):
            response = rdf
        else:    
            params = '&dataset=' + Dataset.get('uri')
            params += "&mime="+urllib.quote_plus("application/rdf+xml")
            params += "&document="+rdf
            
            response = BKNWSF.structwsf_request("crud/update", params,"post",'*/*')
        return response
    
    '''
    /crud/create will add attributes and/or attribute values to an existing record.
    all existing data will remain the same.
    '''
    ##
    @staticmethod
    def add (record, ds_id=''):
        '''
            record should have an id
            SHOULD SET Record.set() id, need to test for recordList or 'id'        
        '''
        response = None
        rdf = None
        bibjson = {}
        bibjson['recordList'] = []
        bibjson['dataset'] = Dataset.template()
        Dataset.set(ds_id)
        bibjson['dataset']['id'] =  Dataset.get('uri') #Dataset.set(ds_id, 'uri')
    
        if (isinstance(record,dict) and ('dataset' in record)):
            # the record is in full bibjson format
            bibjson = record
        elif (isinstance(record,dict)):
            bibjson['recordList'].append(record);                            
        elif (isinstance(record,list)):
            bibjson['recordList'].extend(record)
        else:
            # assume record is a fully-baked dataset already converted to an rdf string
            rdf = record
        
        if (not rdf):
            rdf = BKNWSF.convert_json_to_rdf(bibjson)   

        if(isinstance(rdf,dict) ):
            response = rdf       # the convert failed
        elif (('dataset' in bibjson) and ('id' in bibjson['dataset'])):
            ds = '&dataset=' + Dataset.get('uri')
            mime = "&mime="+urllib.quote_plus("application/rdf+xml")
            doc = "&document="+urllib.quote_plus(str(rdf))
            params = ds + mime + doc
            response = BKNWSF.structwsf_request("crud/create", params,"post",'*/*')    
        return response


    ##
    @staticmethod
    def delete (record_uri, dataset_uri):
        '''
        No default parameters because we don't want inadvertent delete of the current record.
        Parameters can be ids or uris. They are converted to uris
        '''
        if (record_uri and dataset_uri):
            params = ''
            params += '&uri=' + record_uri
            params += '&dataset=' + dataset_uri
            response = BKNWSF.structwsf_request('crud/delete', params, 'get', '*/*')
        else:
            response = {'error': 'No parameters specified for Record.delete()'}

        return response
        

class Test:  
    @staticmethod
    def test_dataset_setting():
        print "Dataset.set test init"
        template = Dataset.part['template']
        print "template:",template
        Dataset.part['template'] = None # make it easier to print part for root, id, and uri
        
        print Dataset.part
        print "Dataset.make_root()"
        Dataset.set(BKNWSF.get()+'datasets/'+'test_makeroot')
        print Dataset.part
    
        print "\nDataset.set(...,'root')"
        Dataset.set(BKNWSF.get()+'datasets/','root')
        print Dataset.part
        print "\nDataset.set(id)", Dataset.set('dataset1')
        print Dataset.part
        print "\nDataset.set(root+id)", Dataset.set(Dataset.get('root')+"dataset2")
        print Dataset.part
        print "\nDataset.set(@@uri)", Dataset.set("@@"+Dataset.part['uri'])
        print Dataset.part
        print "\nDataset.set(@id)", Dataset.set("@"+Dataset.part['id'])
        print Dataset.part
        print "\nDataset.get()", Dataset.get()
        Dataset.part['template'] = template
    
        print "\nRecord.set test init"
        print Record.part
        print "\nRecord.set(id)", Record.set('record1')
        print Record.part
        print "\nRecord.set(ds_uri+id)", Record.set(Dataset.get()+'record2')
        print Record.part
        print "\nRecord.get()", Record.get()
    
        print "\n\nAny side effect to Dataset?"
        print Dataset.part    
        print
        print
        
            
    ##
    @staticmethod
    def autotest(root='http://people.bibkn.org/wsf/'):
        '''
        TODO:
            report pass/fail
            check responses for errors
            automate checking of results
            clean up after errors
            error summary 
        '''
        print 'autotest'
        response = {}
        test_result = {}
        BKNWSF.set(root,'root')
        Service.set(BKNWSF.get()+'ws/','root')    
        Test.test_dataset_setting()
        Dataset.set(BKNWSF.get()+'datasets/','root')
    
        print "Dataset.list('ids') "
        response = Dataset.list('ids')
        if (not response) or ('error' in response): print ':error: '+ str(response)
        #print "skip Dataset.list()"
        response = Dataset.list()
        if (not response) or ('error' in response): print ':error: '+ str(response)
        print Dataset.set('dataset_test')
        print "Dataset.create() ", Dataset.get()
        response = Dataset.create() # this calls auth_registar_access
        if (response): 
            print ':error: '+ str(response)
        else:
            print "Dataset.read() "
            response = Dataset.read() # 'all' returns bad json error
            if (not response) or ('error' in response): print ':error: '+ str(response)
            record_id = '1'
            print "Record.set(record_id)",Record.set(record_id)
            bibjson = {"name": "add","id": record_id}
            print "Record.add() "
            response = Record.add(bibjson)
            if (response): 
                print ':error: '+ str(response)
            else:
                print "Record.read() "
                response = Record.read()
                if (not response) or ('error' in response): 
                    print ':error: '+ str(response)
                else:
                    bibjson = {"name": "update","id": record_id}
                    print "Record.update() "
                    
                    response = Record.update(bibjson)                 
                    if (response): 
                        print ':error: '+ str(response)
                    else:            
                        print "Record.read() "
                        response = Record.read()
                        if (not response) or ('error' in response): 
                            print ':error: '+ str(response)
                    record_id = '2'
                    Record.set(record_id)
                    print "Record.delete() ", Record.get()
                    bibjson = {"name": "this should be deleted","id": record_id}
                    response = Record.add(bibjson)
                    Record.delete(Record.get(), Dataset.get())
                    
    
            print "BKNWSF.browse() "
            response = BKNWSF.browse()
            print simplejson.dumps(response, indent=2)
            
            print "Dataset.delete() "
            response = Dataset.delete()
            if (response): 
                print ':error: '+ str(response)
        return response
    
    
    @staticmethod
    def wsf_test():
        response = {}
        #instance = 'http://www.bibkn.org/wsf/'
        instance = 'http://datasets.bibsoup.org/wsf/'
        Test.autotest(instance)
        BKNWSF.set(instance,'root')
        Service.set(BKNWSF.get()+'ws/','root')    
        Dataset.set(BKNWSF.get()+'datasets/','root')
        #Dataset.set('dataset_test')
    
        #BKNWSF.web_proxy_services(None)
        #response = Dataset.create() # this calls auth_registar_access
        #response = Dataset.delete();
        #init_logging()
        other_params = ''
        Dataset.set('new_jack')
        #response = BKNWSF.browse()
        #response = Dataset.list();
        #response = Dataset.list('ids');
        #response = Dataset.list('details')    
        #response = Dataset.read('all')
        #response = Dataset.read(Dataset.set('')))
        
        #response = create_and_import(Dataset.set('jack_test_create'), 'in.json')    
        #response = Dataset.auth_registrar_access(Dataset.get(), 'create')     
        #response = data_import(Dataset.set('jack_test_create'), 'in.json')
        #response = Record.read(Record.set('1'))  
        #response = BKNWSF.search('Pitman') 
        #response = BKNWSF.search('Pitman', 'all', 10, 0, other_params) 
        #response = BKNWSF.browse(None,1)
        print simplejson.dumps(response, indent=2)
        print '\n'    
        if (('recordList' in response) and response['recordList']):
            # you can get total results by calling
            facets = get_result_facets(response)
            if (facets):
                print 'facets'
                print simplejson.dumps(facets, indent=2)
                print '\nNOTE: not all things are people. See facets[\"type\"]'
                print 'for  counts: (there are a few things to check)'
                print '\t owl_Thing - \t should represent everything if it exists'
                print '\t Object - \t not sure why this does not represent everything'
                print '\t Person - \t just people'
    
    
    
    @staticmethod
    def jim_test():
        response = {}
        BKNWSF.set('http://people.bibkn.org/wsf/','root')
        Service.set(BKNWSF.get()+'ws/','root')    
        Dataset.set(BKNWSF.get()+'datasets/','root')
        ds = Dataset()
        ds.set('mass_import_test3')
    #    print Record.set('1')
        response = ds.delete()
        init_logging()
        other_params = ''    
        
        # Jack Alves modified because Dataset id/uri behavior was updated
        
        #response = create_and_import(ds.part['id'], '/Users/Jim/Desktop/Bibkn/hku_idify.json',testlimit=1,import_interval = 2000)    
        ds_id = 'mass_import_test3'
        ds_uri = Dataset.set(ds_id)
        response = create_and_import(dataset_uri, '/Users/Jim/Desktop/Bibkn/hku_idify.json',testlimit=1,import_interval = 2000)    
        print BKNWSF.browse()
        #response = Dataset.auth_registrar_access(Dataset.get(), 'create')     
        #response = data_import(Dataset.set('jack_test_create'), 'in.json')
        #response = Record.read(Record.set('1'))  
        #response = BKNWSF.search('Pitman') 
        #response = BKNWSF.search('Pitman', 'all', 10, 0, other_params) 
        print simplejson.dumps(response, indent=2)
        print '\n'    
        """
        if (('recordList' in response) and response['recordList']):
            # you can get total results by calling
            facets = get_result_facets(response)
            if (facets):
                print 'facets'
                print simplejson.dumps(facets, indent=2)
                print '\nNOTE: not all things are people. See facets[\"type\"]'
                print 'for  counts: (there are a few things to check)'
                print '\t owl_Thing - \t should represent everything if it exists'
                print '\t Object - \t not sure why this does not represent everything'
                print '\t Person - \t just people'    
        """


'''
TO SEE HTTP REQUEST/RESPONSE set 
deb = 1 in the BKNWSF.structwsf_request function
'''

'''
debug should be replaces by optional logging
'''
def debug (str): 
    print str

def strip_key_prefix(k):
    return re.sub('.*:','', k)

def get_thing_count(response):
    if ('aggregate' in response):
        for a in response['aggregate']:
            if (('object' in a) and ('ref' in a['object'])):
                if (a['object']['ref'][-6:] == '#Thing'):
                    if ('count' in a):
                        return int(a['count'])
    return 0

def get_result_facets(response):
    facets = {}
    if ('aggregate' in response):
        for a in response['aggregate']:
            kind = ''
            name = ''
            full_name = ''
            count = 0            
            if ('count' in a):
                count = int(a['count'])
            
            if (('property' in a) and ('ref' in a['property'])):
                i = a['property']['ref'].rfind('#') + 1 
                kind = a['property']['ref'][i:].lower()
                if (kind not in facets):
                    facets[kind] = {}
            
            if (('object' in a) and ('ref' in a['object'])):
                full_name = a['object']['ref']
                if (full_name[-1] == '/'): # Datasets may have trailing slash
                    full_name = full_name[0:-1]
                i = full_name.rfind('/') + 1 # set i past /
                name = full_name[i:].replace('#', '_') # if pound is found, the property has a name prefix 
                if (name and kind):
                    facets[kind][name] = count
                        
    return facets


##
def data_import(ds_id, datasource, testlimit = None, start=0, import_interval=1):
#How to keep track of what we've imported already?    
    f_hku = codecs.open(datasource,'r', "utf-8")    
    json_str = f_hku.read()
    bibjson = simplejson.loads(json_str)
    f_hku.close()
    bib_import = {}
    bib_import['dataset'] = bibjson['dataset']
    bib_import['dataset']['id'] = Dataset.get('id') #Dataset.get('uri')
# SET TO TEST
    count = 0
    status = {'code': 'ok'}
    Dataset.set(ds_id)
    for i in range(start,len(bibjson['recordList']), import_interval):
        count += 1
        debug( count)
# BREAKS HERE IF TESTING
        if (testlimit and (count > testlimit)) : break      
        bib_import['recordList'] = bibjson['recordList'][i:i+import_interval]
        rdf = BKNWSF.convert_json_to_rdf(bib_import)
        response = Record.add(str(rdf),Dataset.get()) #Dataset.set(ds_id, 'uri')
#If there are any records left, import the rest as the last batch
    if not (testlimit and (count > testlimit)) :  
        bib_import['recordList'] = bibjson['recordList'][i+import_interval:]
        if len(bib_import['recordList'])>0:
            rdf = BKNWSF.convert_json_to_rdf(bib_import)
            response = Record.add(str(rdf),Dataset.get())  # Dataset.set(ds_id, 'uri')
    return status

##
def create_and_import (ds_id, datasource, title='', description='', testlimit = None , import_interval = 1):
    response = Dataset.create(Dataset.set(ds_id), title, description)
    if response:
        debug( 'Error')
        debug( 'Dataset probably exists')
    # Dataset.create calls to set access
    #if (not response): response = Dataset.auth_registrar_access(Dataset.get()) 
    if (not response):
        response = data_import(Dataset.get(), datasource,testlimit=testlimit, import_interval = import_interval)
    return response



# bkn_wsf.py can be used as a web service see BKNWSF.web_proxy_services()
cgitb.enable()
cgi_fields = cgi.FieldStorage()    
if (cgi_fields):
    BKNWSF.web_proxy_services(cgi_fields)
