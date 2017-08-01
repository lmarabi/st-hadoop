/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

var xmlHttp = null;
//var server = "alkhwarizmi-umh.cs.umn.edu"; //"128.101.96.158";
var server = "localhost";
var portnum = "8085";


function processing_on() {
	document.getElementById('loadingImg').style.visibility='visible';
}

function processing_off() {
	document.getElementById('loadingImg').style.visibility='hidden';
}


/*
 *
 * Functions for handeling interface
 */
function DataOperation(){
	if(document.getElementById('join').checked){
		document.getElementById('twitter').checked = false;
		document.getElementById('nyc').checked = false;
		document.getElementById('both').checked = true;
	}else{
	  document.getElementById('both').checked = false;
	}
	clearMap();
}


function clearnycData(){
	if(document.getElementById('join').checked){
		document.getElementById('rq').checked = true;
	}
	document.getElementById('twitter').checked = true;			
	document.getElementById('nyc').checked = false;
	document.getElementById('both').checked = false;
	clearMap();
}

function clearTwitterData(){
	if(document.getElementById('join').checked){
		document.getElementById('rq').checked = true;
	}
	document.getElementById('twitter').checked = false;
	document.getElementById('nyc').checked = true;
	document.getElementById('both').checked = false;
	flyToNYC();
	clearMap();
}


function flyToNYC() {
	initialize();
	var lat =  "40.75934982299805";
	var lon  = "-73.97571563720703";
	epsg4326 =  new OpenLayers.Projection("EPSG:4326"); //WGS 1984 projection
	projectTo = map.getProjectionObject(); //The map projection (Spherical Mercator)
	var lonLat = new OpenLayers.LonLat( lon , lat ).transform(epsg4326, projectTo);
	var zoom=9;
	map.setCenter (lonLat, zoom);
 }

/*
 *
 * Functions for handeling Query and Requests. 
 */

function exportForm() {
	if (boxCoords === null) {
	        alert('No area has been chosen!');
	        return;
	}
	//get the coordinates 
	var coords = document.getElementById("boxC").value.split(',');
	var x2 = coords[0];
	var x1 = coords[1];
	var y2 = coords[2];
	var y1 = coords[3];
	var t1Temp = document.getElementById("t1").value;
	var time1 = t1Temp.split("-");
	var t2Temp = document.getElementById("t2").value;
	var time2  = t2Temp.split("-");
	var t1  = time1[0]+"-"+time1[1]+"-"+time1[2];
	var t2  = time2[0]+"-"+time2[1]+"-"+time2[2];
	var operation = "";
	var shape = "";
	if(document.getElementById('join').checked){
		operation = "join";
	}else{
		operation = "rq";
	}
	if(document.getElementById('nyc').checked){
		shape = "stpoint";
	}else{
		shape = "twitter";
	}
	if(document.getElementById('both').checked){
		shape = "both";
	}
	clearMap();
	executeQuery(x1,y1,x2,y2,operation,shape,t1,t2);
}

function executeQuery(x1,y1,x2,y2,operation,shape,from,to){

	processing_on();
	//http://localhost:8085/query?operation=rq&shape=stpoint&x1=-180&y1=-90&x2=180&y2=90&t1=2015-01-01&t2=2015-01-05
	var Url = "http://"+server+":"+portnum+"/query?"+"operation="+operation+"&shape="+shape+"&x1="+x1+"&y1="+y1+"&x2="+x2+"&y2="+y2+"&t1="+from+"&t2="+to;
	xmlHttp = new XMLHttpRequest(); 
	xmlHttp.onreadystatechange = ProcessRequest;
	xmlHttp.open( "GET", Url, true );
	xmlHttp.send( null );
}


function ProcessRequest() 
{
    if ( xmlHttp.readyState == 4 && xmlHttp.status == 200 ) {
        if ( xmlHttp.responseText == null ) {
         	// do nothing 
        }else{
		var sthadoop = 0;
		var shadoop = 0;
		var hadoop = 0;
		var resultCount = 0; 
	   	var part_sthadoop = 0;
		var part_shadoop = 0;
		var part_hadoop = 0;
		var box_sthadoop = new Array();
		var box_shadoop = new Array();
		var info = eval ( "(" + xmlHttp.responseText + ")" ); 
		//Get the information of the result count 
		for(var key in info.resultCount){
			resultCount = info.resultCount[key].count;
		}

		//Get the information of the STHadoop Partitions
		for (var key in info.STPartitions) {
			//alert('ST:day ='+info.STPartitions[key].day+' mbr ='+info.STPartitions[key].mbr+' cardinality ='+info.STPartitions[key].cardinality);
			sthadoop += info.STPartitions[key].cardinality;
			var mbr = info.STPartitions[key].mbr;
			var xy = mbr.split(',');
			// split on comma and then inserted it in array of float before push it to the array box 
			var box = new Array();
			for( i=0; i< xy.length ; i++){
				box.push(parseFloat(xy[i]));
			}
			box_sthadoop.push(box);
			part_sthadoop += 1;
        	}
		//Get the information of the SpatialHadoop Partitions  
		for (var key in info.SPartitions) {
			//alert('S:day ='+info.SPartitions[key].day+' mbr ='+info.SPartitions[key].mbr+' cardinality ='+info.SPartitions[key].cardinality);
			shadoop += info.SPartitions[key].cardinality;
			var mbr = info.SPartitions[key].mbr;
			var xy = mbr.split(',');
			// split on comma and then inserted it in array of float before push it to the array box 
			var box = new Array();
			for( i=0; i< xy.length ; i++){
				box.push(parseFloat(xy[i]));
			}
			box_shadoop.push(box);
			part_shadoop += 1; 
        	}
		//alert("box_sthadoop: "+box_sthadoop);
		//alert("box_sthadoop: "+box_sthadoop);
		sthadoop -= resultCount; 
		shadoop -= resultCount; 
		hadoop = 1000000000 - resultCount;
		part_hadoop = 81920;
		//Get the few points from the data
		/*for (var key in info.data) {
			alert('data:x ='+info.data[key].x+' y ='+info.data[key].y);
			//markers.push([Number(info.locations[key].x) ,Number(info.locations[key].y),Number(info.locations[key].value)]);		}
        	}
		*/
		//draw Charts. 
		processing_off();
		/* // To fix outlers in the result. 
		var expected = part_shadoop * 0.20;
		var value = Math.ceil(expected);
		var couldbe = part_sthadoop + 20; 
		if( couldbe > value){	
			part_sthadoop = value;
		}

		if(part_shadoop < part_sthadoop){
			var temp = part_sthadoop; 
			part_sthadoop = part_shadoop;
			part_shadoop = temp; 
			
		}
		if(shadoop < sthadoop){
			var temp = sthadoop; 
			sthadoop = shadoop;
			shadoop = temp; 
			
		}
		*/
		drawChart2(part_sthadoop,part_shadoop,part_hadoop);	
		drawChart1(sthadoop,shadoop,hadoop);
		drawChart3(sthadoop,shadoop,hadoop);
		//draw boxes on maps invoke methods
		drawBoxes(box_shadoop,"blue");
		drawBoxes(box_sthadoop,"brown");
		
		                   
    	}
    }

}
 

function drawChart2(part_sthadoop,part_shadoop,part_hadoop){

if(part_sthadoop == 0)
	part_sthadoop = "";
if(part_shadoop == 0)
	part_shadoop = "";

	var chart = new CanvasJS.Chart("chartContainer2",
    {
      title:{
        text: "HDFS Partitions"    
      },
      animationEnabled: true,
      axisY: {
        title: "Number of partitions",
	logarithmic:  true
      },
      axisX:{
      },
      legend: {
        verticalAlign: "bottom",
        horizontalAlign: "center"
      },
      theme: "theme2",
      data: [

      {        
        type: "column",  
        showInLegend: true, 
        legendMarkerColor: "grey",
        legendText: "10 TB Dataset",
        dataPoints: [      
        {y: part_sthadoop, label: "STHadoop"},
        {y: part_shadoop,  label: "SpatialHadoop" },
        {y: part_hadoop,  label: "Hadoop"}
        ]
      }   
      ]
    });
    chart.render();
}

function drawChart1(sthadoop,shadoop,hadoop){

if(sthadoop == 0)
	sthadoop = "";
if(shadoop == 0)
	shadoop = "";

    var chart = new CanvasJS.Chart("chartContainer1",
    {
      title:{
        text: "Scanned Objects"    
      },
      animationEnabled: true,
      axisY: {
        title: "Objects",
	logarithmic:  true
      },
      legend: {
        verticalAlign: "bottom",
        horizontalAlign: "center"
      },
      theme: "theme2",
      data: [

      {        
        type: "column",  
        showInLegend: true, 
        legendMarkerColor: "grey",
        legendText: "1 Billion Dataset",
        dataPoints: [      
        {y: sthadoop, label: "STHadoop"},
        {y: shadoop,  label: "SpatialHadoop" },
        {y: hadoop,  label: "Hadoop"}       
        ]
      }   
      ]
    });

    chart.render();
}


function drawChart3(sthadoop,shadoop,hadoop){
        var st = (sthadoop / 1000000000) * 100;
        var s = (shadoop / 1000000000) * 100;
        var h = (hadoop / 1000000000) * 100;	
	var chart = new CanvasJS.Chart("chartContainer",
	{
		title:{
			text: "Refinement Phase Scans"
		},
                animationEnabled: true,
		legend:{
			verticalAlign: "center",
			horizontalAlign: "left",
			fontSize: 13,
			fontFamily: "Helvetica"        
		},
		theme: "theme2",
		data: [
		{        
			type: "pie",       
			indexLabelFontFamily: "Garamond",       
			indexLabelFontSize: 20,
			indexLabel: "{label} {y}%",
			startAngle:-20,      
			showInLegend: true,
			toolTipContent:"{legendText} {y}%",
			dataPoints: [
				{  y: st, legendText:"STHadoop", label: "STHadoop" },
				{  y: s, legendText:"SpatialHadoop", label: "SpatialHadoop" },
				{  y: 0, legendText:"Hadoop", label: "Hadoop" }
			]
		}
		]
	});
	chart.render();
}



