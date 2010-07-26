
// Query examples:

// Browse a specific dataset
/*
var ws = URLEncode("http://people.bibkn.org/ws/browse/");
var accept = URLEncode("application/json");
var method = "post";
var params = URLEncode("attributes=all&types=all&datasets=http%3A%2F%2Fpeople.bibkn.org%2Fwsf%2Fdatasets%2F117%2F&items=10&page=0&inference=on&include_aggregates=true");
*/


// A search for "Pitman"
var service = "http://people.bibkn.org/ws/search/";
var dataset_root = "http://people.bibkn.org/wsf/datasets";
var wsf_php_proxy = "http://localhost/structwsf/wsproxy.php";

var ws = URLEncode(service);
var accept = URLEncode("application/json");
var method = "post";
var params = URLEncode("query=pitman&datasets="+dataset_root+"/90/;"+dataset_root+"/91/;"+dataset_root+"/106/;"+dataset_root+"/112/;"+dataset_root+"/datasets/114/;"+dataset_root+"/datasets/115/;"+dataset_root+"/datasets/116/;"+dataset_root+"/datasets/117/;"+dataset_root+"/datasets/119/;"+dataset_root+"/datasets/124/;"+dataset_root+"/datasets/125/;"+dataset_root+"/datasets/129/;"+dataset_root+"/datasets/130/;"+dataset_root+"/datasets/132/&items=10&page=0&inference=on&include_aggregates=true");
params = URLEncode("query=pitman");

var reqParams = "ws="+ws+"&accept="+accept+"&method="+method+"&params="+params;

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
			document.getElementById("people").innerHTML = req.responseText;
		}
	}
	
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
				document.getElementById("people").innerHTML = req.responseText;
			}
		}		
		
		req.send(reqParams);
	}
}




function URLEncode(clearString) 
{
  var output = '';
  var x = 0;
  
  clearString = clearString.toString();
  
  var regex = /(^[a-zA-Z0-9_.]*)/;
  
  while (x < clearString.length) 
  {
    var match = regex.exec(clearString.substr(x));
	
    if (match != null && match.length > 1 && match[1] != '') 
	{
		output += match[1];
		x += match[1].length;
    } 
	else 
	{
      if (clearString[x] == ' ') 
	  {
	  	output += '+';
	  }
	  else 
	  {
	  	var charCode = clearString.charCodeAt(x);
	  	var hexVal = charCode.toString(16);
		
	  	output += '%' + (hexVal.length < 2 ? '0' : '') + hexVal.toUpperCase();
	  }
	  
      x++;
    }
  }
  
  return output;
}
