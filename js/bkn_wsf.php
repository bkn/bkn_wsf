<?php

require("WebServiceQuerier.php");
// added this to see if it fixes problem with ie not getting data
header("Cache-Control: no-cache"); 

// main - THIS SHOULD BE USED JUST TO ROUTE SERVICE OR CALLS TO THE APPROPRIATE FUNCTIONS
	$location = "http://people.bibkn.org/wsf/ws/";
	$service = "";
	$ws = "";
	if(isset($_POST["ws"]))
	{
		$ws = $_POST["ws"];
	}
	else 
	{
		if(isset($_POST["location"]))
		{
			$location = $_POST["location"];
		}
		if(isset($_POST["service"]))
		{
			$service = $_POST["service"];
		}
		$ws = $location.$service;
	}

	$method = "post";
	if(isset($_POST["method"]))
	{
		$method = $_POST["method"];
	}
	
	$accept = "text/xml";
	if(isset($_POST["accept"]))
	{
		$accept = $_POST["accept"];
	}

	$params = "";
	if(isset($_POST["params"]))
	{
		$params = $_POST["params"];
	}

	$callback = "";
	if(isset($_POST["callback"]))
	{
		$callback = $_POST["callback"];
	}

// CHECK IF SERVICE MATCHES A WRAPPER THE ALL THE WRAPPER FUNCTION.		
	if ($service == 'test') {
		test($callback, $ws, $method, $accept, $params);	
	}
	else {
		if ($service == 'browse') { 
			$wsq = browse ($params);
		}
		else {
			$wsq = wsf_XMLHttpRequest ($ws, $method, $accept, $params);
		}
		
		header("HTTP/1.1 ".$wsq->getStatus()." ".$wsq->getStatusMessage());	
		header("Content-Type: ".$accept);	
		if($callback)
		{
			echo $callback . '('. $wsq->getResultset() . ')';
		}
		else
		{
			echo $wsq->getResultset();
		}

	
	}
// end main


// FUNCTONS

	// THESE 'get_' FUNCTIONS SHOULD BE OBJECTS WITH SET AND GET METHODS
	function get_dataset_root() {
		return "http://people.bibkn.org/wsf/datasets/";	
	}

	function get_service_root() {
		return "http://people.bibkn.org/wsf/ws/";
	}

	// This is the primary function to call structwsf service
	function wsf_XMLHttpRequest ($ws, $method, $accept, $params) {
	
	
		$registered_ip = "0.0.0.0";
	
		// Get the IP address of the user (JS user) that wants to send the request.
		if(isset($_SERVER['REMOTE_ADDR']))
		{
		  $registered_ip = $_SERVER['REMOTE_ADDR'];
		}	
		
		$wsq = new WebServiceQuerier($ws, $method, $accept, $params."&registered_ip=".$registered_ip);
	
		// Note: if an error occured on the structWSF side, lets forward it to the requester
		// for example, a 403 non-authaurized error! So, everything is channeled with this
		// wsproxy.php script.
	
		return $wsq;    	
	}
	
	//call this function to see parameters passed to structwsf
	// THIS COULD BE IMPLEMENTED AS AN OPTIONAL LAST PARAMETER TO wsf_XMLHttpRequest
	function wsf_testRequest ($ws, $method, $accept, $params) {
	
	
		$registered_ip = "0.0.0.0";
	
		// Get the IP address of the user (JS user) that wants to send the request.
		if(isset($_SERVER['REMOTE_ADDR']))
		{
		  $registered_ip = $_SERVER['REMOTE_ADDR'];
		}	
		

		$wsq = "wsf_testRequest: "."ws=".$ws."  method=".$method." accept=". $accept." params=".$params."&registered_ip=".$registered_ip;
		return $wsq;    
	
	}

	function convert_json_to_rdf($data) {
		$params = "&docmime=".urlencode("application/iron+json")."&document=".$data;
		//$wsq = wsf_testRequest(get_service_root()."converter/irjson/", 
		$wsq = wsf_XMLHttpRequest(get_service_root()."converter/irjson/",
									"post", 
									"application/rdf+xml", 
									$params
									);
		return $wsq;
	}

	function convert_text_xml_to_json($wsq) {	
		$params = "&docmime="."text/xml"."&document=".$wsq->getResultset();
		$wsq = wsf_XMLHttpRequest(get_service_root()."converter/irjson/", 
									"post", 
									"application/iron+json", 
									$params
									);
		return $wsq;
	}
	
// YOU ARE HERE	
	function test($callback, $ws, $method, $accept, $params) {
		$ds_id = "jack_update_test";
		$ds_uri = "http://people.bibkn.org/wsf/datasets/".$ds_id . "/";
		$ds_params = "&dataset=".urlencode($ds_uri);
	    parse_str($params, $other_params); 
	    
	    $doc_json = json_decode($other_params['document']);
	    //$edit_test->name = "F Giasson";
		$wsq = convert_json_to_rdf(urlencode($other_params['document']));	   
		if($wsq->getStatus() == "200"){
			$doc_rdf = $wsq->getResultset();
			// YOU ARE HERE READY TO  TRY crud/update
			$update_params = $ds_params . "&mime=". urlencode("application/rdf+xml");
			$update_params .= "&document=".$doc_rdf;
			
// YOU ARE HERE AND JUST SUCCESSFULLY PERFORMED AN UPDATE
			
			$wsq = wsf_XMLHttpRequest(get_service_root()."crud/update/", 
						"post", 
						"*/*", 
						$update_params
						);
			//$test = $wsq;
		}
					
		if($wsq->getStatus() == "200"){
			$read_params = "&include_linksback=True&include_reification=True";
			$read_params .= $ds_params."&uri=".$ds_uri ."f2";
			
			$wsq = wsf_XMLHttpRequest(get_service_root()."crud/read/", 
	    					"get", 
	    					'application/iron+json', 
	    					$read_params);
		}
	
		//header("HTTP/1.1 ". "400 BKN WSF TEST");	
		header("HTTP/1.1 ".$wsq->getStatus()." ".$wsq->getStatusMessage());	
		header("Content-Type: ".$accept);	
		echo $callback . '('. $wsq->getResultset() . ')';
		//echo $callback . '('. '{"testresult":"'.urlencode($wsq->getResultset()) . '"})';
		//echo $callback . '('. $wsq->getResultset() . ')';
//		echo $callback . '('. json_encode($test) . ')';
		//echo $callback . '('. $test . ')';
		
	}
	
	
// THIS IS NOT READY TO TEST	
/*
	function get_detailed_response($response) {
//		$summary = json_decode($response);
		if ($response && $response->{'dataset'}) {
			$data['dataset'] = $response->{'dataset'};
		}
		foreach ($response->{'recordList'} as $r) {
			if ($r->{'id'} && ($r->{'type'} != 'dataset')) {
			$wsq = wsf_XMLHttpRequest(get_service_root()."crud/read/", "get", 'application/iron+json', $params);
			$data['resultlist'] = $r;
			
			}
		}
		return $wsq;
	}
*/	
	
	
	function browse($other_params) {
		// CHECK IF DS USES FULL PATH OR JUST THE DATASET NAME 
//		$ds = '&datasets=' . get_dataset_root() . urlencode(ds_id) . '/';
		//$params = "&include_aggregates=True";
		if ($other_params) {
			$params = $other_params;
		}
		$wsq = wsf_XMLHttpRequest(get_service_root()."browse/", "post", 'text/xml', $params);
		//data = get_detailed_response(registered_ip, response)
		$wsq = convert_text_xml_to_json($wsq);
		
		return $wsq;
	}
	
?>
