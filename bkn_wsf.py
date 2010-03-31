# coding: utf-8

# This set file incluces wrappers and examples for using Structured Dynamics structWSF,
# a framework for managing an RDF repository using a variety of data formats including BibJSON.
# BibJSON is based on Structured Dynamics JSON-based format called irON. 

# Wrappers and examples do not expose the full functionality of structWSF
# For more information see,
#     http://openstructs.org/structwsf

import re
import codecs
import urllib
import urllib2
import simplejson

def wsf_request (service, params, http_method="post", accept_header="application/json"):
    deb = 0
    if (service[-1] != '/'): service += '/'        
    s = 'http://people.bibkn.org/ws/'+ service
    p = params
    response_format = "json"
    header = {"Accept": accept_header}
    if ((accept_header == 'bibjson') or (accept_header == 'application/iron+json')):
        header['Accept'] = "application/iron+json"
    elif (accept_header == "json"):
        header['Accept'] = "application/json"    
    else:
        response_format = "other"

# This output is helpful to inculde when reporting bugs
    if deb: print '\n\nHEADER:\n', header
    if deb: print '\nREQUEST: \n',s+'?'+p
    response = None
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
        
    if deb: print '\nWSF CALL RESPONSE:\n', response
    try:
        if (response and (type(response) is not dict) and (response_format == "json")):
            response = simplejson.loads(response)
    except: # this catches url and http errors        
        print 'BAD JSON:'
        if (type(response) is not dict): print response.replace('\\n','\n')
        response = {'error':'simplejsonError','reason':'bad json', "response":response}
    
    #print '\nWSF CALL RESPONSE:\n', response        
    return response


def get_wsf_service_root ():
    return 'http://www.bibkn.org/wsf/ws/'

def get_dataset_root ():
    return "http://people.bibkn.org/wsf/datasets/"

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
    if (type(response) is not dict): # only an error would return dict
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

def convert_rdfjson_to_json(rdfjson):
    # This is a rough method to convert an rdf style json to more familiar json
    # This was based on the structure of a search result that returns 'application/json'.
    irjson = {}
    irjson['dataset'] = {}
    if ('prefixes' in rdfjson):
        irjson['dataset']['prefixes'] = rdfjson['prefixes']
    irjson['recordList']= []
    
    
    # NOTE: RDFJSON COULD ALSO HAVE attribute 'subject' or 'subjects' (with an s suffix)
    # same for predicates
    # for now just handling 'subjects' and 'predicates'
    
    #ALSO return should be recordList not resultset
    
    if ('resultset' in rdfjson) and ('subjects' in rdfjson['resultset']):
        rdf_subjects = rdfjson['resultset']['subjects']

        for s in rdf_subjects:
            
            ds = {}
            for rdfkey in s:
                if (rdfkey != 'predicates'):
                    ds[rdfkey] = s[rdfkey]
                else:            
                    for p in s['predicates']:                       
                        for attribute in p:
                            k = strip_key_prefix(attribute)
                            if (k in ds):
                                if (type(ds[k]) != list):
                                    ds[k] = [ds[k]] # convert to list
                                ds[k].append(p[attribute])
                            else:
                                ds[k] = p[attribute]
            irjson['recordList'].append(ds)
    return irjson

'''

FOLLOWING IS A VERY TEMPORARY HACK (hopefully). 

It is a copy of the above function except that 'subject' rather than 'subjects' is parsed.
 and 'predicate' vs 'predicates'

'''
def convert_subject_to_json(rdfjson):
    # This is a rough method to convert 'application/rdf+xml' to more familiar json
    # This was based on the structure of a search result that returns 'application/json'.
    irjson = {}
    irjson['dataset'] = {}
    if ('prefixes' in rdfjson):
        irjson['dataset']['prefixes'] = rdfjson['prefixes']
    irjson['recordList']= []
    
    
    # NOTE: RDFJSON COULD ALSO HAVE attribute 'subject' or 'subjects' (with an s suffix)
    # same for predicates
    # for now just handling 'subjects' and 'predicates'
    
    #ALSO return should be recordList not resultset

    if ('resultset' in rdfjson) and ('subject' in rdfjson['resultset']):
        rdf_subjects = rdfjson['resultset']['subject']

        for s in rdf_subjects:
            
            ds = {}
            for rdfkey in s:
                if (rdfkey != 'predicate'):
                    ds[rdfkey] = s[rdfkey]
                else:            
                    for p in s['predicate']:                       
                        for attribute in p:
                            k = strip_key_prefix(attribute)
                            if (k in ds):
                                if (type(ds[k]) != list):
                                    ds[k] = [ds[k]] # convert to list
                                ds[k].append(p[attribute])
                            else:
                                ds[k] = p[attribute]
            irjson['recordList'].append(ds)
    return irjson


def dataset_create(registered_ip, ds_id, title, description=None, creator=None):
    ip = '&registered_ip=' + registered_ip
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
    services += wsf_service_root+'browse/'
    permissions = '&crud='+urllib.quote_plus('True;True;True;True')
    action = '&action=create'
    params += urllib.quote_plus(services) + permissions + action
    response = wsf_request('auth/registrar/access', params)
    return response

def add_records(registered_ip, ds_id, rdf_str):
    ip = '&registered_ip=' + registered_ip
    ds = '&dataset=' + get_dataset_root() + urllib.quote_plus(ds_id) + '/'
    mime = "&mime="+urllib.quote_plus("application/rdf+xml")
    doc = "&document="+urllib.quote_plus(rdf_str)
    params = ip + ds + mime + doc
    response = wsf_request("crud/create", params,"post",'*/*')
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
    if (type(response) is not dict): # only an error would return dict    
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
    if (type(response) is not dict): # only an error would return dict
        response = convert_text_xml_to_json(ip, response)
    return response

def get_dataset_list(ip, other_params=None):    
# it may be possible to avoid multiple calls by using '&uri=all' with read_dataset

    ds_list = {'recordList':[]}
    params = '&registered_ip='+ip+'&mode=dataset'
    if (other_params): params += other_params
    response = wsf_request("auth/lister", params, "get", 'text/xml')
    if (type(response) is dict): # only an error would return dict
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


def data_import(ip, ds_id, datasource):
    
    f_hku = codecs.open(datasource,'r', "utf-8")    
    json_str = f_hku.read()
    bibjson = simplejson.loads(json_str)
    f_hku.close()
    
    r = bibjson['recordList'][0]
    bib_import = {}
    bib_import['dataset'] = bibjson['dataset']

# SET TO TEST
    testlimit = 5
    count = 0
    status = {'code': 'ok'}
    for r in bibjson['recordList']:
        count += 1

# STOP TEST
        if (testlimit and (count > testlimit)) : break      

        bib_import['recordList'] = []
        bib_import['recordList'].append(r)
        
        f_hku = open('temp.json','w')    
        f_hku.write(simplejson.dumps(bib_import, indent=2))
        f_hku.close()
    
        '''
        xml = convert_json_to_text_xml(ip, bib_import)
        f_hku = open('temp.xml','w')    
        f_hku.write(xml)
        f_hku.close()
        '''      
        rdf = convert_json_to_rdf(ip, bib_import)
        f_hku = open('temp.rdf.xml','w')    
        f_hku.write(rdf)
        f_hku.close()       
        
        response = add_records(ip, ds_id, rdf)
    return status
            
def create_and_import (ip, ds_id, datasource, title=None, description=''):
    t = title
    if (not title): t = ds_id
    
    response = dataset_create(ip, ds_id, t, description)
    if (not response):
        response = auth_registar_access(ip, ds_id) 
    if (not response):
        response = data_import(ip, ds_id, datasource)
    return response

'''
EXAMPLES

To try an example copy and paste one of the 'response=' lines to the end of the
above the simplejson.dumps() call

    REGISTER FOR AN ACCOUNT: http://bibkn.org/drupal/user/register (this is not correct)
    LOGIN: http://www.bibkn.org/user
    REGISTER AN EXTERNAL IP: http://people.bibkn.org/drupal/admin/settings/conStruct/access/
    ----------------------------------------------------
    dataset = 135 # sandbox
    dataset = 117 # AuthorClaim
    dataset = 159 # Hong Kong
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
    # returns dict - JSON with only a few attributes
    # to get more data for each result use read_records()
    response = search('Pitman', ip, None, 10, 0, other_params) 
    
    # READ RECORDS
    # WORKAROUND ISSUE 98 BAD JSON RETURN FROM CONVERTER
    data = read_record(r['id'], registered_ip, ds_id)

    # CREATE DATASET
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
    #ip = "67.20.80.37" # wiggleback.com bluehost server    
    ip = "66.92.4.19"  # Jack's Mac
    other_params = ''
    # These are a few optional parameters used by some services.
    # They are here to make it easier for you to add one or more to a service call.
    #other_params += '&inference=off'
    #other_params += '&include_aggregates=true' # use if you want attrubute counts
    ds_id = '132'
    ds_id = 'hku_test9'
    ds_id = '130' 
    #response = create_and_import(ip, ds_id, 'in.json')
    response = get_dataset_list(ip)
    #response = read_dataset(ip, 'all') # returns bad json error
    print 'Datasets:'
    print simplejson.dumps(response, indent=2)
    #response = browse(ip, ds_id, 10, 0, other_params)     
    response = search('pitman', ip, None, 25, 0, other_params) 
    print
    print simplejson.dumps(response, indent=2)
    
    if ('recordList' in response):
        # you can get total results by calling
        print 'facets'
        facets = get_result_facets(response)
        print simplejson.dumps(facets, indent=2)
        print '\nNOTE: not all things are people. See facets[\"type\"]'
        print 'for  counts: (there are a few things to check)'
        print '\t owl_Thing - \t should represent everything if it exists'
        print '\t Object - \t not sure why this does not represent everything'
        print '\t Person - \t just people'


#wsf_test()