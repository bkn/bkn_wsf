<?php

	require("WebServiceQuerier.php");
	
	$params = "";
	if(isset($_POST["params"]))
	{
		$params = $_POST["params"];
	}
	
	$ws = "";
	if(isset($_POST["ws"]))
	{
		$ws = $_POST["ws"];
	}

	$method = "";
	if(isset($_POST["method"]))
	{
		$method = $_POST["method"];
	}
	
	$accept = "";
	if(isset($_POST["accept"]))
	{
		$accept = $_POST["accept"];
	}
	
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

	header("HTTP/1.1 ".$wsq->getStatus()." ".$wsq->getStatusMessage());	
	header("Content-Type: ".$accept);
//    echo $wsq->getResultset();
    echo $_GET['jsonp'] . '('. $wsq->getResultset() . ')';


?>