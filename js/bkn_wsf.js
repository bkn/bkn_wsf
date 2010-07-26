
    
function deb(str, linebreak){
	var debug_id = document.getElementsByTagName("BODY").item(0);
	if (linebreak == undefined){
	    $(debug_id).append("<br>");        
	}
	$(debug_id).append(str);  
}


function show_json(response){

	deb("RESPONSE as text: " + response);
	deb("RESPONSE as json: " + formattedJSON(response));
}

// wsf_service_reference is used when services are reference in a structwsf parameter
var wsf_service_reference = 'http://people.bibkn.org/wsf/ws/';
function get_wsf_service_reference () {
    return wsf_service_reference;  // GLOBAL
}
function set_wsf_service_reference (path) {
    wsf_service_reference = path; // GLOBAL
}

// wsf_service_root is used to call services
var wsf_service_root = 'http://people.bibkn.org/ws/';
function get_wsf_service_root () {
    return wsf_service_root;  // GLOBAL
}
function set_wsf_service_root (path) {
    wsf_service_root = path; // GLOBAL
}

var dataset_root = 'http://people.bibkn.org/wsf/datasets/';
function get_dataset_root () {
    return dataset_root;
}
function set_dataset_root (path) {
    dataset_root = path;  // GLOBAL
}

function convert_xml_to_string (x) {
	if (window.ActiveXObject) {
		return x.xml;
	 }
	// code for Mozilla, Firefox, Opera, etc.
	else {
	   return (new XMLSerializer()).serializeToString(x);
	}
}


// This is a plain XMLHttpRequest that does not require jquery.
function wsf_XMLHttpRequest (callback, service, params, method, accept) { //

var ws = encodeURIComponent(get_wsf_service_root() + service);
//var wsf_php_proxy = wsf_location+wsf_script;
//accept = encodeURIComponent("application/json");
//params = encodeURIComponent("query=pitman");
params = encodeURIComponent(params);
// END

// A search for "Pitman"
//var service = "http://people.bibkn.org/ws/search/";
var dataset_root = get_dataset_root(); //"http://people.bibkn.org/wsf/datasets";
var wsf_php_proxy = "wsproxy.php";
//wsf_php_proxy = "http://localhost/structwsf/wsproxy.php";
//wsf_php_proxy = "http://downloads.bibsoup.org/services/structwsf/wsproxy.php";


//var ws = encodeURIComponent(service);
//var accept = encodeURIComponent("application/json");
//var method = "post";
//var params = encodeURIComponent("query=pitman&datasets="+dataset_root+"/90/;"+dataset_root+"/91/;"+dataset_root+"/106/;"+dataset_root+"/112/;"+dataset_root+"/datasets/114/;"+dataset_root+"/datasets/115/;"+dataset_root+"/datasets/116/;"+dataset_root+"/datasets/117/;"+dataset_root+"/datasets/119/;"+dataset_root+"/datasets/124/;"+dataset_root+"/datasets/125/;"+dataset_root+"/datasets/129/;"+dataset_root+"/datasets/130/;"+dataset_root+"/datasets/132/&items=10&page=0&inference=on&include_aggregates=true");
//params = encodeURIComponent("query=pitman");

var reqParams = "ws="+ws+"&accept="+accept+"&method="+method+"&params="+params;
deb("http_request:  "+wsf_php_proxy+'?'+reqParams);   

var req;

// branch for native XMLHttpRequest object
if (window.XMLHttpRequest) 
{
	req = new XMLHttpRequest();
	
	req.open("POST", wsf_php_proxy, true);

	req.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
	req.setRequestHeader("Content-length", reqParams.length);
	req.setRequestHeader("Connection", "close");

	req.onreadystatechange = function() {//Call a function when the state changes.
		if(req.readyState == 4 && req.status == 200) {
			//var str = convert_xml_to_string(req.responseText);
			//deb('<br>wsf_XMLHttpRequest response:<br>'+ str);
				callback(req.responseText);
			//deb('<br>wsf_XMLHttpRequest response encoded:<br>'+ encodeURIComponent(req.responseText));
			//document.getElementById("people").innerHTML = req.responseText;
		}
	};
	
	req.send(reqParams);
	
// branch for IE/Windows ActiveX version
} 
else if (window.ActiveXObject) 
{
	req = new ActiveXObject("Microsoft.XMLHTTP");
	
	if (req) 
	{
		req.open("POST", wsf_php_proxy, true);
		
		req.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
		req.setRequestHeader("Content-length", params.reqParams);
		req.setRequestHeader("Connection", "close");		
		
		req.onreadystatechange = function() {//Call a function when the state changes.
			if(req.readyState == 4 && req.status == 200) {
				callback(req.responseText);
				//deb('wsf_XMLHttpRequest response:<br>'+req.responseText);//document.getElementById("people").innerHTML = req.responseText;
//				document.getElementById("people").innerHTML = req.responseText;
			}
		};		
		
		req.send(reqParams);
	}
}


}


// This uses jquery to call structwsf services via a php proxy
function wsf_request (callback, service, params, method, accept) {
//$wsq = new WebServiceQuerier($ws, $method, $accept, $params."&registered_ip=".$registered_ip);

	var wsf_php_proxy = "http://downloads.bibsoup.org/services/structwsf/wsproxy-jsonp.php";
						//"http://downloads.bibsoup.org/services/structwsf/"

//wsf_php_proxy = "http://localhost/structwsf/wsproxy-jsonp.php";
/*
    var wsf_location = "http://";
    wsf_location += (window.location.protocol == "file:") ? "localhost" : window.location.hostname;
    var wsf_script = "/structwsf/wsproxy-jsonp.php";
    wsf_php_proxy = wsf_location + wsf_script;
*/    

    var wsf_params = ""; 
    wsf_params += "&ws=" + get_wsf_service_root()+service;
    wsf_params += "&method=";
    if (method) {wsf_params += method;} else {wsf_params += "post";}
    wsf_params += "&accept=";
    if (accept) {wsf_params += accept;} else {wsf_params += "application/json";}
    
    wsf_params += "&params=";
    if (params) {wsf_params += params;}

deb(wsf_php_proxy +"\?jsonp=?"+wsf_params);   
//return;
    $.ajax({
        url: wsf_php_proxy + "\?jsonp=?", // THE JSONP SUFFIX SHOULD NOT BE NESSARY ANYMORE.
        data: wsf_params,
        type: "post",
        cache: false,
        dataType: "jsonp",
        error: function(xobj, status, error){
            if (xobj.status == '403') {
            	deb("This service requires authentication.");
            	deb("You must login and you must have permission for the service and dataset.");
            	deb("<a href='http://people.bibkn.org/'>Login</a>");
            	deb("<a href='http://people.bibkn.org/conStruct/dataset/'>Datasets</a>");            
            }
            else {
	            deb("<br>status: " + status);
       		    deb("<br>xobj: " + xobj.status);
    	        if (error) {deb("<br>error: " + error);}
            }
        },
        success: callback
    });    
} // end wsf_request

// This calls the bkn_wsf.php wrapper which always returns jsonp.
// THIS COULD BE INTEGRATED WITH THE ABOVE wsf_request FUNCTION
function bkn_wsf_request (callback, service, params, method, accept) {
	var wsf_php_proxy = "";
    var wsf_location = "http://";
    // MOVE TO SERVICES.BIBSOUP
//    wsf_location += (window.location.protocol == "file:") ? "localhost" : window.location.hostname+"/services/";
    wsf_location += (window.location.protocol == "file:") ? "localhost" : window.location.hostname+"/";
    var wsf_script = "structwsf/bkn_wsf.php";
    wsf_php_proxy = wsf_location + wsf_script;

    var wsf_params = ""; 
//    wsf_params += "&ws=" + get_wsf_service_root()+service;
	wsf_params += "&location=" + get_wsf_service_root();
	wsf_params += "&service="+service;
    wsf_params += "&method=";
    if (method) {wsf_params += method;} else {wsf_params += "post";};
    wsf_params += "&accept=";
    if (accept) {wsf_params += accept;} else {wsf_params += "application/json";};
    wsf_params += "&params=";
    if (params) {wsf_params += params;};

	response_format = 'jsonp';
	if (accept) {response_format = accept;};
deb(wsf_php_proxy +"?"+wsf_params);

    $.ajax({
        url: wsf_php_proxy,// + "\?jsonp=?",
        data: wsf_params,
        type: "post",
        cache: false,
        dataType: "jsonp", //response_format,
        error: function(xobj, status, error){
            if (xobj.status == '403') {
            	deb("This service requires authentication.");
            	deb("You must login and you must have permission for the service and dataset.");
            	deb("<a href='http://people.bibkn.org/'>Login</a>");
            	deb("<a href='http://people.bibkn.org/conStruct/dataset/'>Datasets</a>");            
            }
            else {
	            deb("<br>status: " + status);
       		    deb("<br>xobj: " + xobj.status);
    	        if (error) {deb("<br>error: " + error);}
            }
        },
        success: callback
    }); 
}



function make_wsf_param_str(params) {

    var p = "";
    for (k in params) {
        p += k + "=" + params[k];
    }
    
    return encodeURIComponent(p);
}


function dataset_permission(ip) {
    var l = "http://people.bibkn.org"; // dataset location could be different than service?
    var wsf_params = {};
    wsf_params = {
        "&registered_ip": ip,
    	"&crud"         : "False;True;False;False",
    	"&ws_uris"      : l+"/wsf/ws/search/;"+l+"/wsf/ws/browse/;"+l+"/wsf/ws/crud/read/",
    	"&dataset"      : l+"/wsf/datasets/1/",
        "&action"       : "create"
    };
    var s = "auth/registrar/access/" ;
    var p = make_wsf_param_str(wsf_params);
    
    wsf_request(show_json, s, p);
        
}
function get_dataset_ids (other_params) {
    params = '&mode=dataset';
    if (other_params) {params += other_params;};
    wsf_request(show_json, "auth/lister/", params, "get");
}


function get_dataset_list() {
    var param_str = "";
    var wsf_params = {};
    wsf_params = {
//        "&registered_ip": ip,
        "&mode":"access_user"
    };
    param_str = make_wsf_param_str(wsf_params);
    wsf_request(show_json, "auth/lister/", "mode=access_user", "get");
    
}

function dataset_delete(ds_id) {
    //params = '&registered_ip=' + registered_ip;
    var params = "";
    params += '&uri=' + get_dataset_root() + ds_id + '/';
    response = wsf_request(show_json, "dataset/delete", encodeURIComponent(params), "get") ;
    return response;
}

function convert_json_to_rdf(data) {
    var bibjson = { 'dataset': dataset_template,
    				'recordList':[data]
    				};
    var params = "";
    params += "&docmime="+encodeURIComponent("application/iron+json");
    params += "&document="+encodeURIComponent(JSON.stringify(bibjson));
show_json(bibjson);
    wsf_XMLHttpRequest(show_json, "converter/irjson/", params, 
    								"post",encodeURIComponent("application/rdf+xml"));

}

// THIS WAS IMPLEMENTED IN PHP. IT SHOULD NOT BE NESSESSARY IN JAVASCRIPT
function convert_text_xml_to_json(callback, data) {
    var params = "";
    params += "&docmime="+encodeURIComponent("text/xml");
    params += "&document="+encodeURIComponent(data);
    wsf_XMLHttpRequest(function(response) {
    							callback(JSON.parse(response));
    						}, 
    						"converter/irjson/", params, 
    						"post",encodeURIComponent("application/iron+json"));
}

// This calls a PHP structwsf wrapper function
function bkn_wsf_call(callback, call, params) {

	bkn_wsf_request(show_json, call, 
					encodeURIComponent(params), 
					"post", 
					encodeURIComponent("application/iron+json")
					);

}


// MAIN
var dataset_template = {
	"type":"dataset",
	"id": "",
	"schema": [
		"identifiers.json",
		"type_hints.json",
		"http://downloads.bibsoup.org/datasets/bibjson/bibjson_schema.json",
		"http://www.bibkn.org/drupal/bibjson/bibjson_schema.json"
		],				
	"linkage": ["http://www.bibkn.org/drupal/bibjson/iron_linkage.json"]
	};


var ds_id = "";
ds_id = '115'; // mgp - subset - no public permissions
ds_id = '119'; // mgp - full

// YOU HERE HERE
// NEXT implement get_detailed_response in bkn_wsf.php


//bkn_wsf_call(show_json, "browse","&datasets=http://people.bibkn.org/wsf/datasets/hong_kong_university/");
ds_id = 'jack_update_test';
ds_uri = get_dataset_root() + ds_id + '/';
params = '&include_linksback=True&include_reification=True';
params += '&dataset=' + ds_uri;
params += '&uri=' + ds_uri+ 'f2';
edit_test = {
  		  "id":"f2",
	      "type":"Person",
	      "name":"Fred Giasson",
	      "firstword":"Fred",
	      "something_else":"hoorah for fred"
	    };
dataset_template['id'] = ds_uri;
var bibjson = {
		"dataset": dataset_template,
		"recordList": [edit_test]
    }
params += "&document=" + JSON.stringify(bibjson);
// YOU ARE HERE AND JUST SUCCESSFULLY PERFORMED AN UPDATE
bkn_wsf_call(show_json, "test", params);


//params = encodeURIComponent('&mode=dataset');
//bkn_wsf_request(show_json, "auth/lister/", params, "get");
//wsf_request(show_json, "auth/lister/", params, "get");
//wsf_request(show_json, "dataset/read/","uri=http://people.bibkn.org/wsf/datasets/115/");
//wsf_request(show_json, "search/","query=pitman");
//wsf_XMLHttpRequest(show_json, "search/","query=pitman");

// THIS IS AN EXAMPLE OF THE WHY PHP WRAPPERS ARE NECESSARY
// wsf_XMLHttpRequest(
//	function(response){convert_text_xml_to_json(show_json, response)}, //convert_json_to_rdf(response); 
//	"browse/","uri=http://people.bibkn.org/wsf/datasets/119/", "post", "text/xml");
