# coding: utf-8

# This set file incluces wrappers and examples for using Structured Dynamics structWSF,
# a framework for managing an RDF repository using a variety of data formats including BibJSON.
# BibJSON is based on Structured Dynamics JSON-based format called irON. 

# Wrappers and examples do not expose the full functionality of structWSF
# For more information see,
#     http://openstructs.org/structwsf

import re, os, logging
from logging import handlers
import codecs
import urllib
import urllib2
import simplejson
#TODO : add parameter to print curl pasteable command

#Setting up logging
#StreamHandler writes to stdout
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

#print os.getcwd()
wsf_service_root = 'http://people.bibkn.org/wsf/ws/'
dataset_root = 'http://people.bibkn.org/wsf/datasets/'

def get_wsf_service_root ():
    return wsf_service_root
def set_wsf_service_root (rootpath):
    global wsf_service_root
    wsf_service_root = rootpath
def get_dataset_root ():
    return dataset_root
def set_dataset_root (rootpath):
    global dataset_root
    dataset_root = rootpath


def wsf_request (service, params, http_method="post", accept_header="application/json", deb = 0):
    if (service[-1] != '/'): service += '/' 
    # as of 6/8/10 the service root to call services uses /ws/
    # and the service root when referring to a service is /wsf/ws/
    # so the following is a temporary patch       
    s = 'http://people.bibkn.org/ws/'+ service
    #s = get_wsf_service_root() + service
    p = params
    response_format = "json"
    header = {"Accept": accept_header}
    if ((accept_header == 'bibjson') or (accept_header == 'application/iron+json')):
        header['Accept'] = "application/iron+json"
    elif (accept_header == "json"):
        header['Accept'] = "application/json"    
    else:
        response_format = "other"

# This output is helpful to include when reporting bugs
    if deb: logger.debug( '\n\nHEADER:\n', header)
    if deb: logger.debug( '\nREQUEST: \n',s+'?'+p)
    response = None
    #print s+"?"+p
    #print header
    
    try:
        if (http_method == "get"):
            req = urllib2.Request(s+"?"+p, headers = header)        
        else: # use post
            req = urllib2.Request(s, headers = header, data = p)        
        fp = urllib2.urlopen(req)
    except urllib2.HTTPError, err: 
        response = {'error':'HTTPError','reason':err.code}
    except urllib2.URLError, err: 
        response = {'error':'URLError','reason':err.reason}
    except:
        response = {'error':'unknown','reason':None}
    else:
        response = fp.read()
        fp.close()
        
    if deb: logger.debug( '\nWSF CALL RESPONSE:\n', response)
    try:
        if (response and (not isinstance(response, dict)) and (response_format == "json")):
            response = simplejson.loads(response)
    except: # this catches url and http errors        
        logger.debug( 'BAD JSON:')
        if (not isinstance(response, dict)): logger.debug( response.replace('\\n','\n'))
        response = {'error':'simplejsonError','reason':'bad json', "response":response}
    
    #print '\nWSF CALL RESPONSE:\n', response        
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
    
            

def convert_bibtex_to_text_xml(registered_ip, data):
    ip = '&registered_ip=' + registered_ip
    mime = "&docmime="+urllib.quote_plus("application/x-bibtex")
    doc = "&document="+urllib.quote_plus(data)
    params = ip + mime + doc
    response = wsf_request("converter/bibtex/", params, "post","text/xml")
    return response     

def convert_text_xml_to_json(registered_ip, data):
    ip = '&registered_ip=' + registered_ip
    mime = "&docmime="+urllib.quote_plus("text/xml")
    doc = "&document="+urllib.quote_plus(data)
    params = ip + mime + doc
    response = wsf_request("converter/irjson/", params, "post",'bibjson')    
    if (not isinstance(response,dict)): # only an error would return dict
        response = {'error':response}
    return response     

def convert_json_to_text_xml(registered_ip, data):
    ip = '&registered_ip=' + registered_ip
    mime = "&docmime="+urllib.quote_plus("application/iron+json")
    doc = "&document="+urllib.quote_plus(simplejson.dumps(data))
    params = ip + mime + doc
    response = wsf_request("converter/irjson/", params, "post","text/xml")
    return response     

def convert_json_to_rdf(registered_ip, data):
    ip = '&registered_ip=' + registered_ip
    mime = "&docmime="+urllib.quote_plus("application/iron+json")
    doc = "&document="+urllib.quote_plus(simplejson.dumps(data))
    params = ip + mime + doc
    response = wsf_request("converter/irjson/", params, "post","application/rdf+xml")
    return response


def dataset_create(registered_ip, ds_id, title, description=None, creator=None):
    ip = '&registered_ip=' + registered_ip
    #ds = '&uri=' + urllib.quote_plus(get_dataset_root() + ds_id + '/')
    ds = '&uri=' + urllib.quote_plus(get_dataset_root() + ds_id + '/')
    params = ip + ds + '&title=' + urllib.quote_plus(title)
    if (description): params += '&description=' + urllib.quote_plus(description)
    if (creator): params += urllib.quote_plus(creator)       
    response = wsf_request("dataset/create", params, "post") 
    return response

def auth_registar_access(registered_ip, ds_id):
    ip = '&registered_ip=' + registered_ip
    ds = '&dataset=' + get_dataset_root() + urllib.quote_plus(ds_id) + '/'
    params = ip + ds
    params += '&ws_uris='
    wsf_service_root = get_wsf_service_root()
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
    action = '&action=create'
    params += urllib.quote_plus(services) + permissions + action
    response = wsf_request('auth/registrar/access', params)
    return response

def dataset_delete(registered_ip, ds_id):
    params = '&registered_ip=' + registered_ip
    params += '&uri=' + urllib.quote_plus(get_dataset_root() + ds_id + '/')
    response = wsf_request("dataset/delete", params, "get") 
    return response

def add_records(registered_ip, ds_id, rdf_str):
    ip = '&registered_ip=' + registered_ip
    ds = '&dataset=' + get_dataset_root() + urllib.quote_plus(ds_id) + '/'
    mime = "&mime="+urllib.quote_plus("application/rdf+xml")
    doc = "&document="+urllib.quote_plus(rdf_str)
    params = ip + ds + mime + doc
    response = wsf_request("crud/create", params,"post",'*/*')
    return response

def update_record(registered_ip, ds_id, bibjson):
    rdf_str = convert_json_to_rdf(registered_ip, bibjson)
    if(isinstance(rdf_str,dict) ):
        logger.debug('ERROR: update_record: BIBJSON TO RDF FAILED')
        logger.debug( simplejson.dumps(rdf_str, indent=2))
        response = rdf_str
    else:    
        params = '&registered_ip=' + registered_ip
        if (ds_id[0:7] == 'http://'):           #allow ds_id to be a uri or id
            params += '&dataset=' + ds_id
            if (params[:-1] != '/'):
                params += '/'
        else:
            params += '&dataset=' + get_dataset_root() + urllib.quote_plus(ds_id) + '/'
        params += "&mime="+urllib.quote_plus("application/rdf+xml")
        params += "&document="+rdf_str
        response = wsf_request("crud/update", params,"post",'*/*')
    return response

def read_record(rid, registered_ip=None, ds_id=None, other_params=None):
    ds_uri = get_dataset_root() + urllib.quote_plus(ds_id) + '/'
    params = '&include_linksback=True&include_reification=True'
    params += '&dataset=' + ds_uri
    params += '&uri=' + rid
    params += '&registered_ip=' + registered_ip
    if (other_params): params += other_params
        
    response = wsf_request('crud/read', params, 'get', 'bibjson')
    return response

def get_detailed_response(registered_ip, response):
    ip = '&registered_ip=' + registered_ip
    if (not isinstance(response, dict)): # only an error would return dict    
        response = convert_text_xml_to_json(ip, response)   

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
                record = read_record(r['id'], registered_ip, get_dataset_id(r))
                if ('recordList' in record):
                    data['recordList'].extend(record['recordList'])
                elif ('error' in record):
                    data['recordList'].append(record)
    return data

def browse(registered_ip, ds_id, items=10, page=0, other_params=None):
    # This is kind of an export.
    # other params: attributes= &types= &inference=
    params = other_params
    ip = '&registered_ip=' + registered_ip
    ds = '&datasets=' + get_dataset_root() + urllib.quote_plus(ds_id) + '/'
    n = '&items='+str(items)
    offset = '&page='+str(page)
    params = ip + ds + n + offset + '&include_aggregates=True'
    if (other_params): params += other_params
    response = wsf_request("browse", params, 'post', 'text/xml')
    data = get_detailed_response(registered_ip, response)
    return data

def search(query, registered_ip=None, ds_id=None, items=10, page=0, other_params=None):
    # other params: &types= &attributes= &inference= &include_aggregates=
    params = '&query='+query + '&include_aggregates=true'
    if (ds_id): params += '&datasets=' + get_dataset_root() + urllib.quote_plus(ds_id) + '/'
    if (items): params += '&items='+str(items)
    if (page):  params += '&page='+str(page)
    if (registered_ip): ip = '&registered_ip=' + registered_ip
    if (other_params): params += other_params
    response = wsf_request('search', params, 'post', 'text/xml')
    data = get_detailed_response(registered_ip, response)
    return data

def get_dataset_id(r):
    id = None
    if ('isPartOf' in r):
        if ('ref' in r['isPartOf']):
            id_uri = r['isPartOf']['ref'].replace('@@','')
            id = id_uri.replace(get_dataset_root(),'')
            id = id[0:-1] # remove trailing /
    return id

def read_dataset(ip, ds_uri, other_params=None):
    params = '&registered_ip='+ip + '&uri=' + ds_uri
    if (other_params): 
        params += other_params
    else:
        params += '&meta=True' + '&mode=dataset'
        
    response = wsf_request("dataset/read/",params,"get", 'text/xml')
    if (not isinstance(response,dict)):
        response = convert_text_xml_to_json(ip, response)
    return response

def get_dataset_ids(ip, other_params=None):
    params = '&registered_ip='+ip+'&mode=dataset'
    if (other_params): params += other_params
    response = wsf_request("auth/lister", params, "get", 'text/xml')
    if (isinstance(response,dict)): # only an error would return dict
        response = {'error': response}
    else:
        response = convert_text_xml_to_json(ip, response)
    return response

def get_dataset_list(ip, other_params=None):    
# it may be possible to avoid multiple calls by using '&uri=all' with read_dataset

    ds_list = {'recordList':[]}
    params = '&registered_ip='+ip+'&mode=dataset'
    if (other_params): params += other_params
    response = wsf_request("auth/lister", params, "get", 'text/xml')
    if (isinstance(response,dict)):
        ds_list['error'] = response
    else:
        response = convert_text_xml_to_json(ip, response)
        if ('recordList' in response):
            ds_root = get_dataset_root()

            for r in response['recordList']:
                if ('li' in r):
                    for d in r['li']:
                        if ('ref' in d):
                           ds_uri = d['ref'].replace('@@','')
                           if (ds_root in ds_uri):
                               ds = read_dataset(ip, ds_uri)
                               if ('dataset' in ds):
                                   ds_list['recordList'].append(ds['dataset'])       
    return ds_list


def data_import(ip, ds_id, datasource, testlimit = None, start=0):
#How to keep track of what we've imported already?    
    f_hku = codecs.open(datasource,'r', "utf-8")    
    json_str = f_hku.read()
    bibjson = simplejson.loads(json_str)
    f_hku.close()
    
    r = bibjson['recordList'][0]
    bib_import = {}
    bib_import['dataset'] = bibjson['dataset']
    bib_import['dataset']['id'] = get_dataset_root() + urllib.quote_plus(ds_id) + '/'

# SET TO TEST
    count = 0
    status = {'code': 'ok'}
    for i in range(start,len(bibjson['recordList'])):
        count += 1

# STOP TEST
        if (testlimit and (count > testlimit)) : break      

        bib_import['recordList'] = []
        bib_import['recordList'].append(r)
        #print bib_import
        f_hku = open(os.path.join(base_path,'temp.json','w'))
    
        '''
        xml = convert_json_to_text_xml(ip, bib_import)
        f_hku = open('temp.xml','w')    
        f_hku.write(xml)
        f_hku.close()
        '''      
        
        #rdf = convert_json_to_text_xml(ip, bib_import)
        rdf = convert_json_to_rdf(ip, bib_import)
        #print rdf
        f_hku = open(os.path.join(base_path,'temp.rdf.xml','w'))
        f_hku.write(str(rdf))
        f_hku.close()       
        
        response = add_records(ip, ds_id, str(rdf))
    return status
            
def create_and_import (ip, ds_id, datasource, title=None, description=''):
    t = title
    if (not title): t = ds_id
    response = dataset_create(ip, ds_id, t, description)
    if response:
        logger.debug( 'Error')
        logger.debug( 'Dataset probably exists')
    if (not response):
        response = auth_registar_access(ip, ds_id) 
    if (not response):
        response = data_import(ip, ds_id, datasource)
    return response

'''
EXAMPLES

To try an example copy and paste one of the 'response=' lines to the end of the
above the simplejson.dumps() call

    REGISTER FOR AN ACCOUNT: http://people.bibkn.org/drupal/user/register
    LOGIN:                   http://people.bibkn.org/user
    REGISTER AN EXTERNAL IP: http://people.bibkn.org/drupal/admin/settings/conStruct/access/
    ----------------------------------------------------
    dataset = 135 # sandbox
    dataset = 117 # AuthorClaim
    dataset = 130 # IMS Fellows
    dataset = 115 # Math Genealogy - just urls
    dataset = 119 # Math Genealogy - complete
    dataset = 129 # UCB Math Faculty
    dataset = 116 # Oberwolfach Photos
    dataset = 124 # UCB faculty expertise
    dataset = 132 # MRAuth
    dataset = 125 # repec
    
    # GET A LIST OF DATASETS
    response = get_dataset_list(ip) 

    # BROWSE
    response = browse(ip, ds_id, 10, 0, other_params)     

    # SEARCH
    # does search, then read_record for each record
    response = search('Pitman', ip, None, 10, 0, other_params) 
    
    # READ RECORDS
    r['id'] = 'name_of_dataset' # not the full url
    data = read_record(r['id'], registered_ip, ds_id)

    # CREATE AND IMPORT A DATASET
    ip = 'your_ip_address'
    ds_id = 'your_dataset_name'
    bibjson_file = 'your_bibjson_file'
    response = create_and_import(ip, ds_id, bibjson_file)

    # CREATE DATASET
    ip = 'your_ip_address'
    ds_id = 'your_dataset_name'
    response = dataset_create(ip, ds_id, 'jack test', 'small test of create and import')

    # SET PERMISSONS
    response = auth_registar_access(ip, ds_id) 

    # ADD RECORDS
    response = add_records(ip, ds_id, rdf)
    
    # UPDATE RECORDS 
    not tested
    
    # DELETE RECORDS
    not tested
    
    # DATASET UPDATE
    not tested
    
    # DATASET DELETE
    not tested
'''

def wsf_test():
    '''
        TO SEE HTTP REQUEST/RESPONSE set 
        deb = 1 in the wsf_request function        
    '''
    
    
    other_params = ''
    ip = "66.92.4.19"  # Jack's Mac
    ds_id = 'jack_import_test13'
    #response = get_dataset_ids(ip)
    #response = get_dataset_list(ip)
    #response = create_and_import(ip, ds_id, 'in.json')
    #response = read_dataset(ip, 'all') # returns bad json error
    #response = browse(ip, ds_id, 10, 0, other_params)      
    response = search('Pitman', ip, None, 25, 0, other_params) 
    
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
