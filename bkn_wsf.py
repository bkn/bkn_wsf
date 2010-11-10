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


'''
 CHANGES since last check-in:

'''


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
# bkn_wsf.py can be used as a web service see BKNWSF.web_proxy_services()
cgitb.enable()

#print os.getcwd()


## Configure debug logging
#
class Logger():
    level = 0
    @staticmethod
    def set(v, k):
        if (v and (k == 'level')):
            Logger.level = v
        return Logger.level

    @staticmethod
    def get(k):
        response = Logger.level
        # planning to support more keys
        if (k == 'level'):
            response = Logger.level
        return response
    
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
    '''
    '''
    part = {
        'root': '',
        'user_ip': os.getenv("REMOTE_ADDR"),
         
         # TODO: GET THE IP OF THE BKNWSF root
         
        'drupal_ip': '184.73.164.129'
            }

    if ((not part['user_ip']) or (part['user_ip'] == '::1')): 
        # This means we are executing on localhost
        # We need to get the external ip address for the local machine
        bkn_wsf = 'http://services.bibsoup.org/cgi-bin/structwsf/bkn_wsf.py'
        part['user_ip'] = str(urllib2.urlopen(bkn_wsf,'&service=get_remote_ip').read()).strip()
        #urllib.urlopen('http://www.whatismyip.com/automation/n09230945.asp').read()

    ##
    @staticmethod
    def set (value, k):    
        '''
        Set the root of the structwsf system. This is generally the domain name with the suffix /wsf.
        Before attempting any operation, the structwsf instance (repository) root must be set.
        bkn_wsf uses the root to construct roots for structwsf service calls and for datasets. 
        '''
        if (k == 'root'):
            BKNWSF.part[k] = slash_end(value)
        return BKNWSF.get()
    @staticmethod
    def get(k='root'):
        '''
        Get the root of the structwsf system.  This is generally the domain name with the suffix /wsf.
        '''
        response = ''
        if (k == 'root'):
            response = BKNWSF.part['root'] 
        elif (k == 'drupal_ip'):
            response = BKNWSF.part['drupal_ip']
        elif (k == 'user_ip'):
            response = BKNWSF.part['user_ip'] 
        return response
    @staticmethod
    def structwsf_request (service, params, http_method="post", accept_header="application/json"):
        '''
        This is the single point of access to structWSF services.
        '''
        deb = Logger.get('level')
#        if deb:
#            response = BKNWSF.structwsf_request_curl (service, params, http_method, accept_header, deb = 1)
#            return response
        
        if (service[-1] != '/'): service += '/' 
        s = str(Service.get('root') + service).strip()
        # the registered_ip= param has different meaning for this auth service
        p = params
        if (service != 'auth/registrar/access/'):    
            p += str('&registered_ip='+BKNWSF.get('user_ip')).strip()
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
#        Test.test_data['wsf'].append({'request':''+s+'?'+p})

        return response
    
    ##
    @staticmethod
    def structwsf_request_curl (service, params, http_method="post", accept_header="application/json", deb = 0):
        '''
        This will be deprecated and a curl option will be added to the primary structwsf_request 
        '''
        if (service[-1] != '/'): service += '/' 
        # as of 6/8/10 the service root to call services uses /ws/
        # and the service root when referring to a service is /wsf/ws/
        #s = 'http://people.bibkn.org/ws/'+ service
        s = Service.get('root') + service
        p = params + '&registered_ip='+BKNWSF.get('user_ip')
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
        This facilitates use of bkn_wsf from client-side javascript applications.
        '''
#        Test.test_data['wsf'] = []
       
        if 0: # TO TEST SET, if 1:
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
                print str(BKNWSF.get('user_ip')),
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
            if(not bkn_root or (bkn_root == '')):
                error = True
                response['error'] = 'bkn_root was not specified.'
            else:
                BKNWSF.set(bkn_root,'root') # 'http://people.bibkn.org/wsf/'
        
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
            elif service == 'dataset_list_ids':
                response = Dataset.list('id_list')  
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
                    response['params'] = urllib.quote_plus(cgi_fields.getfirst('params'))
                else:
                    response = {} # need to make sure to return a dict not ""
#                    response['wsf'] = Test.test_data['wsf']
#                    response['user_ip'] = BKNWSF.get('user_ip')
                    
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
#                    error=True
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
#            if error:
#                response['params'] = urllib.quote_plus(cgi_fields.getfirst('params'))
#            response['testdata'] = []
#            response['testdata'].append(Test.test_data)
            #TEST DEBUG OUTPUT
#            response['service'] =  service            
#            response['dataset_uri'] = Dataset.get()
#            response['record_uri'] = Record.get()
            #TEST DEBUG OUTPUT
        
            print 'Content-type: text/plain \n\n'
            print callback+'('+simplejson.dumps(response)+')'

    ##
    @staticmethod
    def search(query, ds_uris='', items=10, page=0, other_params='&include_aggregates=true'):
        '''
        ds_uris can accept more than one uri and defaults to current dataset if none is passed.
        Unlike many bkn_wsf methods Browse does not call 'Dataset.set' to make the ds_uri the current/default
        dataset uri. Use 'all' to specify all datasets
        
        The associated structWSF service supports filtering results by specified types and attributes.
        'other_params' can be used to specify filtering parameters.
        
        Use 'items' for page size.
        Increment the 'page' parameter to page through results. 
        '''

        if not ds_uris: ds_uris = Dataset.get('uri')
        params = '&query=' + query
        # ds_uris can accept multiple uris so we don't want to update the Dataset object
        # Default to curremt datast if none is passed
        # This means 'all' needs to be explicitly passed to search all datasets
        params += '&datasets=' + ds_uris
        if (items): params += '&items='+str(items)
        start = str(int(items)*int(page)) 
        # the structwsf page param is really an item start value
        if (page):  params += '&page='+str(start)
        # other params: &types= &attributes= &inference= &include_aggregates=
        params += other_params
        response = BKNWSF.structwsf_request('search', params, 'post', 'text/xml')
        data = Dataset.get('record_data',response)
        return data

    ##
    @staticmethod
    def browse(ds_uris='', items=10, page=0, other_params='&include_aggregates=True'):
        '''
        Browse is a kind of export service. A common use is to page through datasets. 
        Returns irjson with list of records for specified datasets.
        
        ds_uris can accept more than one uri and defaults to current dataset if none is passed.
        Unlike many bkn_wsf methods Browse does not call 'Dataset.set' to make the ds_uri the current/default
        dataset uri. Use 'all' to specify all datasets
        
        The associated structWSF service supports filtering results by specified types and attributes.
        'other_params' can be used to specify filtering parameters.

        Use 'items' for page size.
        Increment the 'page' parameter to page through results. 
        '''
        
        params = ''        
        if (not ds_uris): 
            ds_uris = Dataset.get('uri')
        params += '&datasets=' + ds_uris
        if (items): params += '&items='+str(items)
        start = str(int(items)*int(page)) 
        # the structwsf page param is really an item start value
        if (page):  params += '&page='+str(start)
        # other params: attributes= &types= &inference=
        params += other_params
        response = BKNWSF.structwsf_request("browse", params, 'post', 'text/xml')
        data = Dataset.get('record_data',response)
        return data    
##
class Service:
    '''
    This may be deprecated and merged into BKNWSF. 
    This is currently used for the root structwsf service uri which is typically the bkn_wsf root with /ws
    '''
    part = {
        'root': ''
            }
    ##
    @staticmethod
    def set (value, k):
        '''
        '''
        if (k == 'root'):
            Service.part[k] = slash_end(value)
        return Service.get()
    ##
    @staticmethod
    def get(v='root'):
        '''
        '''
        return Service.part[v]
    
    
##
class Dataset:
    '''
    A primary class for operations related to a specified dataset.
    '''    
#    TODO: CHECK FOR VALID PATHS - NO SPACES OR WEIRD CHARS

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
    def set (value, k='', access='read_only'):
        '''
        Set the current uri by specifying either a uri or an id. In either case, the other is constructed
        by adding or removing the dataset root. When setting uri or id, the root will not be updated 
        unless it is None. k can be used to explicity specify the value is a root, uri or id. In each case, the returns 
        value is the uri.
        
        k is also used to to request the permission setting service. When used for permissions 
        the parameter specifies the permission operation to be performed. The 'default_access' option 
        adds full permission for the drupal server associated with the repository. 'public_access' sets
        read_only permission for 0.0.0.0 but the 'access' can be used to set other permissions. The 
        access parameter can be one of,
        
            'read_only'    - read is True, other permissions are False
            'read_update'  - read and update are True, create and delete are False 
            'restricted'   - all access False (even though there is a permission record)
            'full'         - all access True              
        '''
        
        # Allow call with value = None, return current uri but don't do anything else
        # 
        response = Dataset.get()
        if (value and (not k)) : # value is either a uri or id
            if ((value[0:2] == '@@') or (value[0:7] == 'http://')): # value is a uri
                Dataset.set(value, 'uri')
            else: # value is an id
                Dataset.set(value,'id')            
            response = Dataset.get()
        elif (value and (k in Dataset.part)):
            if(k == 'id'):
                if (not Dataset.part['root']):
                    Dataset.make_root(Dataset.part['uri'])
                Dataset.part['id'] =  unslash_end(value.replace('@','',2))
                Dataset.part['uri'] =  slash_end(Dataset.part['root'] + Dataset.part['id'])                
            elif (k == 'uri'):
                Dataset.part['uri'] =  slash_end(value.replace('@','',2))
                if (not Dataset.part['root']):
                    Dataset.make_root(Dataset.part['uri'])
                Dataset.part['id'] = unslash_end(Dataset.part['uri'].replace(Dataset.part['root'],''))
            elif (k == 'root'):
                Dataset.part['root'] = slash_end(value)
            response = Dataset.get()
        elif (value and (k == 'default_access')):
            response = Dataset.default_access(value)
        elif (value and (k == 'public_access')):
            response = Dataset.public_access(value, access)

        return response            
    
    ##
    @staticmethod
    def get(k='uri', v=''):
        '''
        k used to request the current dataset uri, id, or root. k is also used to parse a response from 
        the structWSF browse and search services which return only lists of ids. 
        
        Dataset.get('record_data') iterates through a list extracting record uris and calling
        Record.read to get attributes and values. When used in this way, the v parameter should be
        the text/xml response from browse or search. An irjson list with all record_data is returned.
        '''
        
        def get_dataset_info(response):
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
        
        if (k == 'record_data'):
            response = get_dataset_info(v)
        else:
            response = Dataset.part[k]
        return response    


    ##
    @staticmethod
    def read(ds_uri='', detail='description', other_params=''):
        '''
         detail can be: 
          'description' (default) - list with id, creation date, title, and description
          'access' - list with 'description' and access permissions
          'access_detail' - list with 'access' including list of services
         
         bkn_wsf reformats the structwsf response. When access is specified an atttribute called 
         'concise' is added to describe common access cases with a single value.
         
            'read_only'    - read is True, other permissions are False
            'read_update'  - read and update are True, create and delete are False 
            'restricted'   - all access False (even though there is a permission record)
            'full'         - all access True              
          
        '''    
        params = '&uri='
        if (ds_uri == 'all'):
            params += 'all'    # as of 8/30/2010 structwsf returns a bad response for 'all'
        else:
            params += Dataset.set(ds_uri) # let set figure out if we have an id or uri
        if (other_params): 
            params += other_params
        else:
            params += '&meta=True'
            
        response = BKNWSF.structwsf_request("dataset/read/",params,"get", 'text/xml')
        if (not isinstance(response,dict)):
            response = BKNWSF.convert_text_xml_to_json(response)
            # info is returned in the 'dataset' attribute not 'recordList'
            # it gets moved to 'recordList' here for consistency with list records 
            if ('dataset' in response) and isinstance(response['dataset'],dict):
                ds_detail = response['dataset']
                if (detail == 'access') or (detail == 'access_detail'):
                    ds_detail['access'] = Dataset.access(ds_uri, detail)
                if ('recordList' not in response):
                    response['recordList'] = {}             
                
                response['recordList'] = ds_detail                    
#                response['recordList'].append(ds_detail)                    
                # clear the 'dataset' attribute because it is the same as 'recordList'
                response['dataset'] = {}
                
        return response

    ##
    @staticmethod
    def delete(ds_uri):
        '''
        Delete removes the dataset without warning. The current dataset is not used as a default.
        '''
        if (ds_uri):
            params = '&uri=' + urllib.quote_plus(Dataset.set(ds_uri))
            response = BKNWSF.structwsf_request("dataset/delete", params, "get") 
        else:
            response = {'error': 'No parameters specified for Record.delete()'}
        return response
        
    ##
    @staticmethod
    def create(ds_id='', title='', description=''):
        '''
        Create a dataset and give it default access which is full access for the creator and the
        Drupal server associated with the repository. 
        '''
        ds = '&uri=' + Dataset.set(ds_id) # urllib.quote_plus(Dataset.set(ds_id))
        # Parameter default for title is not used because ds_id may be passed in
        if (not title): title = Dataset.get('id') 
        params = ds + '&title=' + urllib.quote_plus(title)
        if (description): params += '&description=' + urllib.quote_plus(description)
        response = BKNWSF.structwsf_request("dataset/create", params, "post") 
        # the create operation gives read_update to creator
        if (not response):
            # give drupal server read_update
            response = Dataset.set(Dataset.get(), 'default_access')
            pass     
        return response
    
    ##
    @staticmethod
    def public_access(ds_id='', k='read_only'):
        # this may need to check if there is already an access record and do an update
        instance_ip = '0.0.0.0'
        response = Dataset.auth_registrar_access(ds_id, 'create', instance_ip, 'read_only')
        return response

    ##
    @staticmethod
    def default_access(ds_id=''):
        # the create operation gives read_update to creator        
        # defer public access until creator makes an explicit request
        instance_ip = BKNWSF.get('drupal_ip') #'184.73.164.129'
        response = Dataset.auth_registrar_access(ds_id, 'create', instance_ip, 'full')
        instance_ip = BKNWSF.get('user_ip')
        if (instance_ip != BKNWSF.get('drupal_ip')):
            response = Dataset.auth_registrar_access(ds_id, 'create', instance_ip, 'full')
        
        return response
    ##
    @staticmethod
    def auth_registrar_access(ds_id='', action='create', action_ip='', access='read_only', access_uri=''):
        '''
        Set or update permissions for a specified ip address. The access parameter can be,

            'read_only'    - read is True, other permissions are False
            'read_update'  - read and update are True, create and delete are False 
            'full'         - all access True              
            'no_delete'    - all access True, expect for delete              

        If action='update' an  access_uri is expected. 
        
        '''
        wsf_service_root = Service.get()
        services = ''
        services += wsf_service_root+'crud/create/;'
        services += wsf_service_root+'crud/read/;'
        services += wsf_service_root+'crud/update/;'
        services += wsf_service_root+'crud/delete/;'
        services += wsf_service_root+'search/;'
        services += wsf_service_root+'browse/;'
        services += wsf_service_root+'dataset/read/;'
        services += wsf_service_root+'dataset/delete/;'
        services += wsf_service_root+'dataset/create/;'
        services += wsf_service_root+'dataset/update/;'
        services += wsf_service_root+'converter/irjson/;'
#        services += wsf_service_root+'auth/registrar/ws/;'
#        services += wsf_service_root+'auth/registrar/access/;'
#        services += wsf_service_root+'auth/lister/;'
#        services += wsf_service_root+'auth/validator/;'
#        services += wsf_service_root+'import/;'
#        services += wsf_service_root+'export/;'
#        services += wsf_service_root+'ontology/create/;'
        services += wsf_service_root+'sparql/'
        services = '&ws_uris='+urllib.quote_plus(services)
        if (access == 'full'):
            permissions = '&crud='+urllib.quote_plus('True;True;True;True')            
        elif (access == 'read_update'):
            permissions = '&crud='+urllib.quote_plus('False;True;True;False')            
        elif (access == 'read_only'):
            permissions = '&crud='+urllib.quote_plus('False;True;False;False')
        elif (access == 'no_delete'):
            permissions = '&crud='+urllib.quote_plus('True;True;False;False')
        else: # 'restricted'
            permissions = '&crud='+urllib.quote_plus('False;False;False;False')
        if (action_ip == '') : 
            registered_ip = '&registered_ip='+urllib.quote_plus(BKNWSF.get('user_ip')) #.strip()
        else :
            registered_ip = '&registered_ip='+urllib.quote_plus(action_ip.strip())
        ds = '&dataset=' + Dataset.set(ds_id)            
        params = ds + services + '&action='+action + permissions + registered_ip
        if (access_uri):
            params += '&target_access_uri='+access_uri
        response = BKNWSF.structwsf_request('auth/registrar/access', params)
        return response
    
    ##
    @staticmethod
    def access (ds_id='', k='concise'):
        '''
        '''
        ##
        # k = 'access_detail' returns list with all services, otherwise list removes service list
        params = '&mode=access_dataset'
        params += '&dataset='+Dataset.set(ds_id)
        response = BKNWSF.structwsf_request("auth/lister/", params, "get", 'text/xml')
        if (isinstance(response,dict)): # only an error would return dict
            response = {'error': response}
        else:
            response = BKNWSF.convert_text_xml_to_json(response)
            if response:
                ds_access = {}
                if isinstance(response,dict) and ('recordList' in response):
                    for r in response['recordList']:
                        if ('registeredIP' in r):
                            registered_ip = str(r['registeredIP'])
                            if (registered_ip in ds_access):
                                registered_ip = registered_ip+'::'+microtime_id()
                            ds_access[registered_ip] = r
                            # we don't need to return the list of services.
                            if (k != 'access_detail') and ('webServiceAccess' in ds_access[registered_ip]):
                                del ds_access[registered_ip]['webServiceAccess']
                            concise = ''
                            if ('create' in r) and ('read' in r) and ('update' in r) and ('delete' in r):                                    
                                if (r['create'] == 'True') and (r['read'] == 'True') and (r['update'] == 'True') and (r['delete'] == 'True'):
                                    concise = 'full'
                                elif (r['update'] == 'True') and (r['read'] == 'True'):
                                    concise = 'read_update'
                                elif (r['read'] == 'True'):
                                    concise = 'read_only'
                                if (r['create'] == 'False') and (r['read'] == 'False') and (r['update'] == 'False') and (r['delete'] == 'False'):
                                    concise = 'restricted'
                                ds_access[registered_ip]['concise'] = concise
                response = ds_access
        return response
        
    
    ##
    @staticmethod
    def list(v='description', other_params=''):
        '''
        Returns a list of datasets with specified information. v is used to specify the level of detail.
        
            'id' - simple list of ids 
            'description' - list with id, creation date, title, and description
            'access'(default) - list with 'description' and access permissions
            'access_detail' - list with 'access' including list of services       
            
        Note: As of this writing, operations with more detail than 'id' can take a long time 
        (8+ seconds) to respond because multiple requests are made for each dataset.
        This may be resolved in the near future.         
        '''
        
        def get_dataset_detail_for_ref(response, ds_ref, detail='access'):
            ds_uri = ds_ref.replace('@@','')
            ds_root = Dataset.get('root')            
            # this test for root filters out some datasets, 
            # might not want to do this if root is not standard
            # better to explicitly look for datasets we want to exclude
            if (ds_root and (ds_uri.find(ds_root) != -1) and ('recordList' in response)):
                # The following could be replaced after new Dataset.read feature is tested
                # Dataset.read(ds_uri, v)
                # id_list just reformats data so the uri is the id value
                if (v == 'id_list'): 
                    ds = {'recordList':{'id':ds_uri}}
                else:
                    ds = Dataset.read(ds_uri, detail)                
                if ('recordList' in ds) and isinstance(ds['recordList'],dict):
#                    ds_detail = ds['recordList']
#                    if (detail != 'description'):ds_detail['access'] = Dataset.access(ds_uri, detail)
                    response['recordList'].append(ds['recordList']) #(ds_detail)                            
 
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
                # v == 'description', 'access', and 'access_detail' handled by get_dataset_detail_for_ref() 
                ds_root = Dataset.get('root')
                response = {'recordList':[]}
                if ('dataset' in ds_ids):
                    response['dataset'] = ds_ids['dataset']
                for r in ds_ids['recordList']:
                    if ('li' in r):
                        # response is updated by get_dataset_detail_for_ref()
                        # TODO: return response instead of updating
                        if (isinstance(r['li'],dict) and ('ref' in r['li']) and (r['li']['ref'])):
                            get_dataset_detail_for_ref(response, r['li']['ref'], v)
                        else: # it should be an array
                            for d in r['li']:
                                if ('ref' in d) and (d['ref']):
                                    get_dataset_detail_for_ref(response, d['ref'], v)
        return response

    ##
    @staticmethod
    def template():
        '''
        Return a default irjson template
        '''
        return Dataset.part['template']
    

##
class Record:
    '''
    A primary class for operations related to a specified record.
    '''
#    TODO: CHECK FOR VALID PATHS - NO SPACES OR WEIRD CHARS

    part = {
        'uri': '',
        'id': ''   
        }
    ##
    @staticmethod
    def set (value, k=''):        
        '''
        Set the record uri.  if an id is passed, a uri will be constructed using the current dataset uri.
        The uri is returned.
        '''
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
        '''
        Return the current record uri
        '''
        response = Record.part['uri']
        if (k and (k == 'id')):
            response = Record.part['id']            
        return response 

    ##
    @staticmethod
    def read(record_id='', ds_id='', other_params=''):
        '''
        read a single record from specified dataset.
        Defaults to read current record from current dataset
        '''

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
        update is like performing a record delete and create.
        Record must be valid irjson format with or without the 'dataset' section. If the 'dataset'
        section is not included the default template will be used.
        
        The current dataset is used if none is specified.
        
        Record should have an id. structwsf constructs the record uri by prepending the dataset uri. 
        If a record uri is inadvertently used for the id then the id will look like a uri because
        structwsf still prepends the dataset uri.            
        '''

#        TODO: test for valid 'id' in record

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
    
    ##
    @staticmethod
    def add (record, ds_id=''):
        '''
        Create a new record or add attributes and values to an existing record.
        
        If Record.add the record id already exists the new record data adds to the existing data. 
        If you send record data that contains an attribute in the existing record, 
        and the value of the attribute is different, then that value is added to the attribute 
        (the value becomes an array of values). (All structwsf data is ultimately stored as RDF triples. 
        In the case described, a new triple is written.)        

        Record must be valid irjson format with or without the 'dataset' section. If the 'dataset'
        section is not included the default template will be used.
        
        The current dataset is used if none is specified.
        
        Record should have an id. structwsf constructs the record uri by prepending the dataset uri. 
        If a record uri is inadvertently used for the id then the id will look like a uri because
        structwsf still prepends the dataset uri.            

        '''
        
#        TODO: check for valid id
        
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
        Remove a record. There are no default parameters. 
        URIs must be specified. Ids are not converted to uris.
        '''
        
#        TODO: accept Ids

        if (record_uri and dataset_uri):
            params = ''
            params += '&uri=' + record_uri
            params += '&dataset=' + dataset_uri
            response = BKNWSF.structwsf_request('crud/delete', params, 'get', '*/*')
        else:
            response = {'error': 'No parameters specified for Record.delete()'}

        return response
        

class Test:  
    test_data = {} # use this for debugging web_proxy services. Log data then include in response
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
        
    def test_dataset_list():
        
        response = Dataset.list('ids');
        response = Dataset.list('description')    
        response = Dataset.list()    
        response = Dataset.list('access')    
        response = Dataset.list('access_detail')    
        
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
        error = False
        dataset_created = False
        print 'autotest'
        response = {}
        test_result = {}
        BKNWSF.set(root,'root')
        Service.set(BKNWSF.get()+'ws/','root')    
        Test.test_dataset_setting()
        Dataset.set(BKNWSF.get()+'datasets/','root')
    
        print "Dataset.list('ids') "
        response = Dataset.list('ids')
        if (not response) or ('error' in response): 
            error = True
            print ':error: '+ str(response)
        #print "skip Dataset.list()"
        print "Dataset.list() "
        response = Dataset.list()
        if (not response) or ('error' in response): 
            error = True
            print ':error: '+ str(response)
        
        
        print Dataset.set('dataset_test_'+microtime_id()) #need unique name for multi-user use
        print "Dataset.create() ", Dataset.get()
        try:
            response = Dataset.create() # this calls auth_registar_access
            if (response): 
                error = True
                print ':error: '+ str(response)
            else:
                dataset_created = True
                print "Dataset.read() "
                response = Dataset.read() # 'all' returns bad json error
                if (not response) or ('error' in response): 
                    error = True
                    print ':error: '+ str(response)
                record_id = '1'
                print "Record.set(record_id)",Record.set(record_id)
                bibjson = {"name": "add","id": record_id}
                print "Record.add() "
                response = Record.add(bibjson)
                if (response): 
                    error = True
                    print ':error: '+ str(response)
                else:
                    print "Record.read() "
                    response = Record.read()
                    if (not response) or ('error' in response): 
                        error = True
                        print ':error: '+ str(response)
                    else:
                        bibjson = {"name": "update","id": record_id}
                        print "Record.update() "
                        
                        response = Record.update(bibjson)                 
                        if (response): 
                            error = True
                            print ':error: '+ str(response)
                        else:            
                            print "Record.read() "
                            response = Record.read()
                            if (not response) or ('error' in response): 
                                error = True
                                print ':error: '+ str(response)
                        record_id = '2'
                        Record.set(record_id)
                        print "Record.delete() ", Record.get()
                        bibjson = {"name": "this should be deleted","id": record_id}
                        response = Record.add(bibjson)
                        if (response): 
                            error = True
                            print ':error: '+ str(response)
                        
                        Record.delete(Record.get(), Dataset.get())
                        if (response): 
                            error = True
                            print ':error: '+ str(response)
                        
        
            print "BKNWSF.browse() "
            response = BKNWSF.browse()
            if (not response) or ('error' in response): 
                error = True
                print ':error: '+ str(response)
            print simplejson.dumps(response, indent=2)
         
        finally: # always check and report error,  then clean up if a dataset was created
            if error:
                print '\n\nTEST FAILED.\n'
                print 'An error should be display immediately after the test that failed.'

            else:
                print '\n\nTEST PASSED.\n'
                
            if dataset_created:
                print '\n\nCleaning up ...'
                print "Dataset.delete() "
                response = Dataset.delete(Dataset.get())
                if (response):
                    error = True 
                    print '\n\nTEST FAILED.\n'
                    print ':error: '+ str(response)
        
        
        
        return response
    
    
    @staticmethod
    def wsf_test():
        response = {}
        #instance = 'http://www.bibkn.org/wsf/'
        instance = 'http://datasets.bibsoup.org/wsf/'
        Logger.set(0,'level')

        Test.autotest(instance)
        
        BKNWSF.set(instance,'root')
        Service.set(BKNWSF.get()+'ws/','root')    
        Dataset.set(BKNWSF.get()+'datasets/','root')
        Dataset.set('dataset_test')
        #BKNWSF.web_proxy_services(None)


#        response = BKNWSF.search('Pitman') 
#        response = BKNWSF.search('Pitman', 'all', 10, 0, other_params) 
#        response = BKNWSF.browse(None,1)
        
#        Dataset.set('dataset_test_'+microtime_id()) #need unique name for multi-user use
#        response = Dataset.create() 
#        record_id = '1'
#        bibjson = {"name": "test","id": record_id, "type":"Object", "status":"tested"}
#        response = Record.add(bibjson)                 
#        response = Record.read(Record.set('1'))  
#        bibjson = {"name": "update test","id": Record.get('id'), "type":"Object", "status":"tested"}
#        response = Record.update(bibjson)                 
#        response = BKNWSF.browse()        
            
#        response = Dataset.set(Dataset.get(), 'default_access')    
#        response = Dataset.set(Dataset.get(), 'public_access')
#        instance_ip = '184.73.164.129'
#        instance_ip = '0.0.0.0'
#        response = Dataset.auth_registrar_access(Dataset.get(), 'update', instance_ip, 'read_update', access_uri)        


        #response = create_and_import(Dataset.set('jack_test_create'), 'in.json')    
        #response = data_import(Dataset.set('jack_test_create'), 'in.json')
        
#        response = Dataset.list('ids');
#        response = Dataset.list('description')    
#        response = Dataset.list('access')    
#        response = Dataset.list('access_detail')    
#        response = Dataset.read('all') #THIS GIVE BAD JSON ERROR
        

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
    bib_import['dataset']['id'] = Dataset.get('uri') # Dataset.get('id')
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
else:
    global logger
    logger = Logger()

