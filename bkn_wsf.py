#!/usr/bin/env python
# coding: utf-8

# This set file incluces wrappers and examples for using Structured Dynamics structWSF,
# a framework for managing an RDF repository using a variety of data formats including BibJSON.
# BibJSON is based on Structured Dynamics JSON-based format called irON. 

# Wrappers and examples do not expose the full functionality of structWSF
# For more information see,
#     http://openstructs.org/structwsf
'''
changes
8/10/2010 
MAJOR REFACTORING OF FUNCTIONS INTO CLASSES AND METHODS.
General changes
- bkn_wsf.py can now be used as a web service. Not all structwsf calls are supported yet.
- BKNWSF gets the user's ip address, 
- set register_ip parameter in wsf_request for all services, and removed all use of ip address in methods. 
- added autotest
wsf_request
- get the ip and use it for all calls, 
Record.read
- removed include_linksback and include_reification
Dataset.create
- added call to set auth
- use ds_id if no title
Record.add
- modified to accept a simple json record when using Dataset.template()
- not backward compatible because parameters are swapped so ds_id can be optional

Dataset 
YOU MUST EXPLICITLY INITIALIZE THE WSF_SERVICE and DATASET ROOT
THE DATASET URI CAN BE ANYTHING. BY CONVENTION WE APPEND /datasets/ 
TO THE /wsf/ ROOT. THEN APPEND A NAME FOR EACH DATASET.
- new object to store info about a dataset
- replaces get_ / set_ dataset() functions
- created to allow using service for multiple structwsf instance 
- Dataset.set('http://people.bibkn.org/wsf/datasets/', 'root')

Record
- new object to store info about a record

'''

    
'''
EXAMPLES

To try an example copy and paste one of the 'response=' lines to the end of the
above the simplejson.dumps() call

    REGISTER FOR AN ACCOUNT: http://people.bibkn.org/drupal/user/register
    LOGIN:                   http://people.bibkn.org/user
    REGISTER AN EXTERNAL IP: http://people.bibkn.org/drupal/admin/settings/conStruct/access/
    ----------------------------------------------------    
    response = browse(ds_id, 10, 0, other_params)     
    response = search('Pitman', None, 10, 0, other_params) 
    response = Record.read(record_id, ds_id)
    response = create_and_import(ds_id, bibjson_file)
    response = Dataset.create(ds_id, 'jack test', 'small test of create and import')    
    response = Dataset.auth_registrar_access(ds_id) # SET PERMISSONS
'''
        




import re, os, logging
from logging import handlers
from os import getenv,path
from datetime import *
import codecs
import urllib
import urllib2
import urlparse
#from urlparse import parse_qs
import simplejson

import sys
import cgi, cgitb 
cgitb.enable()

#print os.getcwd()



#Setting up logging
#StreamHandler writes to stdout
logger = None
def init_logging ():
    global logger
    base_path = os.path.abspath("")
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s\
     - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    #handler writes to file
    log_filepath = os.path.join(base_path, "log.txt")
    handler = logging.handlers.RotatingFileHandler(\
        log_filepath, maxBytes=1048576, backupCount=5)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addHandler(ch)


def slash_end (s):
    if (s[-1] != '/'): s += '/'
    return s

def unslash_end (s):    
    # use rstrip?
    if (s[-1] == '/'): s = s[0:len(s)-2]
    return s


class BKNWSF:
    part = {
        'root': None,
        'ip': os.getenv("REMOTE_ADDR")
            }

    if ((not part['ip']) or (part['ip'] == '::1')): 
        # This means we are executing locally
        # We need to get the external ip address for the local machine
        bkn_wsf = 'http://services.bibsoup.org/cgi-bin/structwsf/bkn_wsf.py'
        part['ip'] = str(urllib2.urlopen(bkn_wsf,'&service=get_remote_ip').read())
        #urllib.urlopen('http://www.whatismyip.com/automation/n09230945.asp').read()

            
    @staticmethod
    def set (value, k):
        if (k == 'root'):
            BKNWSF.part[k] = slash_end(value)
        return BKNWSF.get()
    @staticmethod
    def get():
        return BKNWSF.part['root']
    @staticmethod
    def ip(format=None):
        if (format == 'param'):
            response = ''
            if (BKNWSF.part['ip']):
                response = '&registered_ip='+BKNWSF.part['ip']
        else:
            response = BKNWSF.part['ip']
        return response


class Service:
    part = {
        'root': None
            }
    @staticmethod
    def set (value, k):
        if (k == 'root'):
            Service.part[k] = slash_end(value)
        return Service.get()
    @staticmethod
    def get(v='root'):
        return Service.part[v]
    
    
class Dataset:
    '''
    CHECK FOR VALID PATHS - NO SPACES OR WEIRD CHARS
    '''
    part = {
        'root': None,
        'uri': None,
        'id': None,
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
    def set (value, k=None):
        # A None value may be passed when methods are called without a ds_id
        # Allow call with value = None but don't do anything
        if (value and (not k)) : # value is either a uri or id
            if (value[0:7] == 'http://'): # value is a uri
                Dataset.part['uri'] =  slash_end(value)
                root_end = Dataset.part['uri'].rfind('/',0,-1) + 1
                Dataset.part['id'] = Dataset.part['uri'][root_end:-1] # withoutslash
                Dataset.part['root'] = Dataset.part['uri'][0:root_end] # with slash
            else: # value is an id
                Dataset.part['id'] = unslash_end(value)
                Dataset.part['uri'] =  slash_end(Dataset.part['root'] + Dataset.part['id'])
        elif (value and (k in Dataset.part)):
            if(k == 'id'):
                Dataset.part[k] = unslash_end(value)
            else:
                Dataset.part[k] = slash_end(value)

        return Dataset.get()            
    
    @staticmethod
    def get(k='uri', v=None):
        def get_detailed_response(response):
            if (not isinstance(response, dict)): # only an error would return dict    
                response = convert_text_xml_to_json(response)   
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
                        Dataset.set(extract_dataset_id_from_browse_response(r))
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


    @staticmethod
    def browse(ds_id=None, items=10, page=0, other_params=None):
        # This is kind of an export.
        # other params: attributes= &types= &inference=
        params = other_params
        ds = '&datasets=' + Dataset.set(ds_id)
        n = '&items='+str(items)
        offset = '&page='+str(page)
        params = ds + n + offset + '&include_aggregates=True'
        if (other_params): params += other_params
        response = wsf_request("browse", params, 'post', 'text/xml')
        data = Dataset.get('detail',response)
        return data    

    @staticmethod
    def read(ds_uri=None, other_params=None):
        params = '&uri=' + Dataset.set(ds_uri)
        if (other_params): 
            params += other_params
        else:
            params += '&meta=True' + '&mode=dataset'
            
        response = wsf_request("dataset/read/",params,"get", 'text/xml')
        if (not isinstance(response,dict)):
            response = convert_text_xml_to_json(response)
        return response

    @staticmethod
    def delete(ds_id=None):
        params = '&uri=' + urllib.quote_plus(Dataset.set(ds_id))
        response = wsf_request("dataset/delete", params, "get") 
        return response
    
    @staticmethod
    def create(ds_id=None, title=None, description=None, creator=None):
        ds = '&uri=' + urllib.quote_plus(Dataset.set(ds_id))
        # Parameter default for title is not used because ds_id may be passed in
        if (not title): title = Dataset.get('id') 
        params = ds + '&title=' + urllib.quote_plus(title)
        if (description): params += '&description=' + urllib.quote_plus(description)
        if (creator): params += urllib.quote_plus(creator)       
        response = wsf_request("dataset/create", params, "post") 
        if (not response):
            response = Dataset.auth_registrar_access(Dataset.get())     
        return response
    
    @staticmethod
    def auth_registrar_access(ds_id=None, action='create'):
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
        response = wsf_request('auth/registrar/access', params)
        return response
    
    @staticmethod
    def ids(other_params=None):
        params = '&mode=dataset'
        if (other_params): params += other_params
        response = wsf_request("auth/lister", params, "get", 'text/xml')
        if (isinstance(response,dict)): # only an error would return dict
            response = {'error': response}
        else:
            response = convert_text_xml_to_json(response)
        return response
    
    @staticmethod
    def list(v='detail', other_params=None):    
    # it may be possible to avoid multiple calls by using '&uri=all' with Dataset.read
        response = None
        params = '&mode=dataset'
        if (other_params): params += other_params
        ds_ids = wsf_request("auth/lister", params, "get", 'text/xml')
        if (isinstance(ds_ids,dict)):
            # a dict means there was an error
            response = ds_ids
        else:
            ds_ids = convert_text_xml_to_json(ds_ids)
            
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
                               if (ds_root in ds_uri):
                                   ds = Dataset.read(ds_uri)
                                   if ('dataset' in ds):
                                       response['recordList'].append(ds['dataset'])       
        return response

    @staticmethod
    def template():
        return Dataset.part['template']
    

class Record:
    '''
    CHECK FOR VALID PATHS - NO SPACES OR WEIRD CHARS
    '''
    part = {
        'uri': None,
        'id': None   
        }
    @staticmethod
    def set (value):        
        if (value) : # value should be either a uri or id            
            if (value[0:7] == 'http://'): # value is a uri            
                Record.part['uri'] = unslash_end(value)
                id_start = Record.part['uri'].rfind('/',0) + 1
                Record.part['id'] = Record.part['uri'][id_start:]
                Dataset.set(Record.part['uri'][0:id_start-1])
            else:
                Record.part['id'] =  unslash_end(value)
                Record.part['uri'] = Dataset.part['uri'] + Record.part['id']         
        return Record.part['uri']

    @staticmethod
    def get ():
        return Record.part['uri']

    @staticmethod
    def read(record_id=None, ds_id=None, other_params=None):
        params = ''
        #params += '&include_linksback=True&include_reification=True'
        #allow ds_id to be a uri or id
        
        params += '&uri=' + Record.set(record_id) # do this first so dataset uri gets set
        params += '&dataset=' + Dataset.set(ds_id)
        if (other_params): params += other_params
        response = wsf_request('crud/read', params, 'get', 'bibjson')
        return response
    
    @staticmethod
    def update(record, ds_id=None):
        '''
            SHOULD SET Record.set() id, need to test for recordList or 'id'
        '''
        response = None
        rdf = None
        bibjson = {}
        bibjson['recordList'] = []
        bibjson['dataset'] = Dataset.template()
        bibjson['dataset']['id'] = Dataset.get('uri')
    
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
            rdf = convert_json_to_rdf(bibjson)   

        if(isinstance(rdf,dict) ):
            response = rdf
        else:    
            params = '&dataset=' + Dataset.set(ds_id)
            params += "&mime="+urllib.quote_plus("application/rdf+xml")
            params += "&document="+rdf
            response = wsf_request("crud/update", params,"post",'*/*')
        return response
    
    '''
    /crud/create will add attributes and/or attribute values to an existing record.
    all existing data will remain the same.
    '''
    @staticmethod
    def add (record, ds_id=None):
        '''
            SHOULD SET Record.set() id, need to test for recordList or 'id'        
        '''
        response = None
        rdf = None
        bibjson = {}
        bibjson['recordList'] = []
        bibjson['dataset'] = Dataset.template()
        bibjson['dataset']['id'] = Dataset.get('uri')
    
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
            rdf = convert_json_to_rdf(bibjson)   

        if(isinstance(rdf,dict) ):
            response = rdf
        else:            
            ds = '&dataset=' + Dataset.set(ds_id)
            mime = "&mime="+urllib.quote_plus("application/rdf+xml")
            doc = "&document="+urllib.quote_plus(str(rdf))
            params = ds + mime + doc
            response = wsf_request("crud/create", params,"post",'*/*')    
        return response


'''
TO SEE HTTP REQUEST/RESPONSE set 
deb = 1 in the wsf_request function
'''

'''
debug should be replaces by optional logging
'''
def debug (str): 
    print str

def wsf_request (service, params, http_method="post", accept_header="application/json", deb = 0):
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
    if deb: debug( '\n\nHEADER:\n'+str(header))
    if deb: debug( '\nREQUEST: \n'+s+'?'+p)
    response = None
    #print s+"?"+p
    #print header
    
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
        response = {'error':'unknown','reason':None}
    else:
        response = fp.read()
        fp.close()
        
    if deb: debug( '\nWSF CALL RESPONSE:\n'+ str(response))
    try:
        if (response and (not isinstance(response, dict)) and (response_format == "json")):
            response = simplejson.loads(response)
    except: # this catches url and http errors        
        if deb: debug( 'BAD JSON:')
        if (not isinstance(response, dict)): debug( response.replace('\\n','\n'))
        response = {'error':'simplejsonError','reason':'bad json', "response":response}
    
    #print '\nWSF CALL RESPONSE:\n', response        
    return response

def convert_bibtex_to_text_xml(data):
    mime = "&docmime="+urllib.quote_plus("application/x-bibtex")
    doc = "&document="+urllib.quote_plus(data)
    params = mime + doc
    response = wsf_request("converter/bibtex/", params, "post","text/xml")
    return response     

def convert_text_xml_to_json(data):
    mime = "&docmime="+urllib.quote_plus("text/xml")
    doc = "&document="+urllib.quote_plus(data)
    params = mime + doc
    response = wsf_request("converter/irjson/", params, "post",'bibjson')    
    if (not isinstance(response,dict)): # only an error would return dict
        response = {'error':response}
    return response     

def convert_json_to_text_xml(data):
    mime = "&docmime="+urllib.quote_plus("application/iron+json")
    doc = "&document="+urllib.quote_plus(simplejson.dumps(data))
    params = mime + doc
    response = wsf_request("converter/irjson/", params, "post","text/xml")
    return response     

def convert_json_to_rdf(data):
    mime = "&docmime="+urllib.quote_plus("application/iron+json")
    doc = "&document="+urllib.quote_plus(simplejson.dumps(data))
    params = mime + doc
    response = wsf_request("converter/irjson/", params, "post","application/rdf+xml")
    return response



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
                



def extract_dataset_id_from_browse_response(r):
    id = None
    if ('isPartOf' in r):
        if ('ref' in r['isPartOf']):
            id_uri = r['isPartOf']['ref'].replace('@@','')
            id = id_uri.replace(Dataset.get('root'),'')
            if (id[-1] == '/'):
                id = id[0:-1] # remove trailing /
    return id

def search(query, ds_uris=None, items=10, page=0, other_params=None):
    if not ds_uris: ds_uris = Dataset.get('uri')
    print 'search call get'+Dataset.get()
    print 'search dsuris:'+ds_uris
    # other params: &types= &attributes= &inference= &include_aggregates=
    params = '&query='+query + '&include_aggregates=true'
    # ds_uris can accept multiple uris so we don't want to update the Dataset object
    # Default to curremt datast if none is passed
    # This means 'all' needs to be explicitly passed to search all datasets
    params += '&datasets=' + ds_uris
    if (items): params += '&items='+str(items)
    if (page):  params += '&page='+str(page)
    if (other_params): params += other_params
    response = wsf_request('search', params, 'post', 'text/xml')
    data = Dataset.get('detail',response)
    return data


def data_import(ds_id, datasource, testlimit = None, start=0):
#How to keep track of what we've imported already?    
    f_hku = codecs.open(datasource,'r', "utf-8")    
    json_str = f_hku.read()
    bibjson = simplejson.loads(json_str)
    f_hku.close()
    
    bib_import = {}
    bib_import['dataset'] = bibjson['dataset']
    bib_import['dataset']['id'] = Dataset.get('uri')

# SET TO TEST
    count = 0
    status = {'code': 'ok'}
    for i in range(start,len(bibjson['recordList'])):
        r = bibjson['recordList'][i]
        count += 1
        debug( count)
# STOP TEST
        if (testlimit and (count > testlimit)) : break      

        bib_import['recordList'] = []
        bib_import['recordList'].append(r)
        rdf = convert_json_to_rdf(bib_import)
        
        response = Record.add(str(rdf),Dataset.set(ds_id))
    return status

def create_and_import (ds_id, datasource, title=None, description=''):
    response = Dataset.create(Dataset.set(ds_id), title, description)
    if response:
        debug( 'Error')
        debug( 'Dataset probably exists')
    # Dataset.create calls to set access
    #if (not response): response = Dataset.auth_registrar_access(Dataset.get()) 
    if (not response):
        response = data_import(Dataset.get(), datasource)
    return response




def get_bkn_wsf_param(cgi_fields, key):
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
    
def autotest():
    print 'autotest'
    test_result = {}
    print "Dataset.list('ids') "
    response = Dataset.list('ids')
    if (not response) or ('error' in response): print ':error: '+ str(response)
    print "Dataset.list()"
    '''
    skip temporarily
    '''
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
        print Record.set(record_id)
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
        print "Dataset.browse() "
        response = Dataset.browse()
        print simplejson.dumps(response, indent=2)
        
        print "Dataset.delete() "
        response = Dataset.delete()
        if (response): 
            print ':error: '+ str(response)
    return response

def wsf_test():
    response = {}
    BKNWSF.set('http://people.bibkn.org/wsf/','root')
    Service.set(BKNWSF.get()+'ws/','root')    
    Dataset.set(BKNWSF.get()+'datasets/','root')
    
    autotest()
    #Dataset.set('jack_update_test')
    #print Dataset.set('mgp_id')  
    #response = Dataset.browse()
    if 0:
        response = Dataset.set('dataset_test')
        print Record.set('1')
        response =  Record.read()
        response = Dataset.delete()
        response = Dataset.read()
    
    #init_logging()
    other_params = ''
    #Dataset.set('jack_update_test')
    #ds_id = Dataset.part['id']
    #record_id = ds_id+microtime_id()
    #record_id = ds_id+"lucky69"
    #response = get_dataset_ids()
    #response = create_and_import(ds_id, 'in.json')
    #response = data_import(ds_id, 'in.json')
    #response = Dataset.read(ds_id) # 'all' returns bad json error 
    #Dataset.set('jack_test_create')  
    #response = Record.read(Record.set('ja1'))  
    Dataset.set('jack_test_create')  
    #response = create_and_import(Dataset.set('jack_test_create'), 'in.json')    
    #response = Dataset.auth_registrar_access(Dataset.get(), 'create')     
    #response = data_import(Dataset.set('jack_test_create'), 'in.json')
    #response = Record.read(Record.set('1'))  
    #response = search('Pitman') 
    #response = search('Pitman', 'all', 10, 0, other_params) 
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

#wsf_test()
#data_import("OpenLibrary_MathStat","/Users/Jim/Desktop/openlibrary_dedup.json",start=111606)
#data_import("Sand","/Users/Jim/Desktop/openlibrary_dedup.json",start=111606)


cgi_fields = cgi.FieldStorage()    

callback = None
if 'callback' in cgi_fields: callback = cgi_fields.getfirst('callback') 

if not callback: 
    if (('service' in cgi_fields) and (cgi_fields.getfirst('service') == 'get_remote_ip')):
        #don't semd end of line for print. We want to return a single line string      
        print 'Content-type: text/plain \n\n',
        print str(BKNWSF.ip()),    
else:       
    '''
    YOU MUST EXPLICITLY INITIALIZE THE WSF_SERVICE and DATASET ROOT 
    '''
    BKNWSF.set('http://people.bibkn.org/wsf/','root')
    Service.set(BKNWSF.get()+'ws/','root')
    Dataset.set(BKNWSF.get()+'datasets/','root')
    #json = urllib.unquote( cgi_fields.getfirst(k)) 
    
    service = cgi_fields.getfirst('service')
    response = {}
    dataset_uri = get_bkn_wsf_service_param(cgi_fields,'dataset')
    record_uri =  get_bkn_wsf_service_param(cgi_fields,'uri')
    bibjson = get_bkn_wsf_service_param(cgi_fields,'document')
    if (bibjson):
        bibjson = simplejson.loads(bibjson)
    
    ds = Dataset()
    r = Record()
    if (dataset_uri): 
        ds.set(dataset_uri)
    if (record_uri): 
        r.set(record_uri)

        
    if service == 'test':
        ds.set('jack_update_test')             
    elif service == 'browse':
        '''
        ADD FORMATTED FACETS TO RETURN
        ''' 
        response = ds.browse()
    elif service == 'read_record':
        response = r.read(r.set(record_uri))
    elif service == 'update_record':   
        
        #response = {'ds':ds.get(), 'r': r.get(), 'doc':bibjson}
        response = r.update(bibjson)
        if (response): # should be None if update succeeded
            response['params'] = cgi_fields.getfirst('params')
        else:
            response = r.read(r.set(record_uri))
        
    #response = simplejson.dumps(bigd)#.replace('\n','<br>').replace('  ','&nbsp;&nbsp;')
    #if 'jsonp' in cgi_fields: callback  = cgi_fields.getfirst('jsonp') 
    #response = browse(ds_id, 10, 0, '')      
    print 'Content-type: text/plain \n\n'
    print callback+'('+simplejson.dumps(response)+')'
