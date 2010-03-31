# coding: utf-8

import re
import codecs
import urllib
import urllib2
import simplejson
from bkn_wsf import *


wsf_test()

if 0:
    #ip = "67.20.80.37" # wiggleback.com bluehost server    
    ip = "66.92.4.19"  # Jack's Mac
    other_params = ''
    # These are a few optional parameters used by some services.
    # They are here to make it easier for you to add one or more to a service call.
    #other_params += '&inference=off'
    #other_params += '&include_aggregates=true' # use if you want attrubute counts
    ds_id = 'hku_test_10'
    ds_id = '132'
    ds_id = '130' 
    #response = create_and_import(ip, ds_id, 'in.json')
    #response = get_dataset_list(ip)
    #response = read_dataset(ip, 'all') # returns bad json error
    response = browse(ip, ds_id, 10, 0, other_params)     
    #response = search('pitman', ip, None, 2, 0, other_params) 
    
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
    
    
    