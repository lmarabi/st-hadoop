/*
 *  Place all the global Var
 **/
//open layer map and draw controls
var map , drawControls, vector;
//An array that stores the locations of points in a path
var pathlocs = new Array();
//An index used to add to the pathlocs array
var index=0;
//Marker layer
var markers = new OpenLayers.Layer.Markers( "Markers" );
var markerend, markerstart;
var lineLayer;
var boxCoords =null;

/*
 *  Initialize the current location of the user 
 **/
function osm_tools_userLocation(){
    var style = {
        fillColor: '#000',
        fillOpacity: 0.1,
        strokeWidth: 0
    };
     
    map = new OpenLayers.Map("osm-map");
    var layer = new OpenLayers.Layer.OSM();
    vector = new OpenLayers.Layer.Vector('vector');
    markers = new OpenLayers.Layer.Markers( "Markers" );
    lineLayer = new OpenLayers.Layer.Vector("Line Layer");
    map.addLayers([layer, vector, markers, lineLayer]);
    //map.addLayers(layer);

 var glayers=[new OpenLayers.Layer.Google("Google Streets",{numZoomLevels: 50}),new OpenLayers.Layer.Google("Google Hybrid",{type: google.maps.MapTypeId.HYBRID,numZoomLevels: 50})];
map.addLayers(glayers);
//Add ESRI layer
var Elayer = new OpenLayers.Layer.XYZ( "ESRI",
                    "http://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/${z}/${y}/${x}",
                    {sphericalMercator: true} );
            map.addLayer(Elayer);

map.addControl(new OpenLayers.Control.LayerSwitcher());
    	map.addControl(new OpenLayers.Control.MousePosition());
        //map.zoomToMaxExtent(); 

    map.setCenter(
        new OpenLayers.LonLat(-71.147, 42.472).transform(
            new OpenLayers.Projection("EPSG:4326"),
            map.getProjectionObject()
            ), 2
        );
    function osm_tools_initMap(){
        map = new OpenLayers.Map("osm-map", {
            eventListeners: {
                moveend: osm_get_current_map,
                zoomend: osm_get_current_map
            }
        });
        map.addLayer(new OpenLayers.Layer.OSM());
    }            
    /**
     * Move the display map to current browser location
    
    function osm_goto_current_location() {
        if (navigator.geolocation) {
            navigator.geolocation.getCurrentPosition(osm_goto_position, location_error);
        } else {
            alert("Geolocation not provided");
        }
    }
 */
    /**
     * Retrieves map data from OSM that are currently displayed on the OpenLayers map
     */
    function osm_get_current_map() {
    
        // Retrieve current map data
        var fromProjection = new OpenLayers.Projection("EPSG:4326");   // Transform from WGS 1984
        var toProjection   = new OpenLayers.Projection("EPSG:900913"); // to Spherical Mercator Projection
        var map_extent = map.getExtent().transform(toProjection, fromProjection);
        osm_get_map(map_extent.left, map_extent.bottom, map_extent.right, map_extent.top);
    }

    /**
     * Get the data currently shown on the OpenLayers map
     
    function osm_get_map(left, bottom, right, top) {
        var url_call = GET_MAP_URL;
        MapBounds = {
            west:left, 
            south:bottom, 
            east:right, 
            north:top
        };
        url_call += "?bbox="+left+","+bottom+","+right+","+top;

    }


    var geolocate = new OpenLayers.Control.Geolocate({
        bind: false,
        geolocationOptions: {
            enableHighAccuracy: false,
            maximumAge: 0,
            timeout: 7000
        }
    });
    map.addControl(geolocate);
    var firstGeolocation = true;
    
    
    geolocate.events.register("locationupdated",geolocate,function(e) {
        vector.removeAllFeatures();
        var circle = new OpenLayers.Feature.Vector(
            OpenLayers.Geometry.Polygon.createRegularPolygon(
                new OpenLayers.Geometry.Point(e.point.x, e.point.y),
                e.position.coords.accuracy/2,
                40,
                0
                ),
                {},
            style
            );
        vector.addFeatures([
            new OpenLayers.Feature.Vector(
                e.point,
                {},
                {
                    graphicName: 'cross',
                    strokeColor: '#f00',
                    strokeWidth: 0,
                    fillOpacity: 0,
                    pointRadius: 10
                }
                ),
            circle
            ]);
        if (firstGeolocation) {
            map.zoomToExtent(vector.getDataExtent());
            firstGeolocation = false;
            this.bind = true;
        }
    });
    
    
    geolocate.events.register("locationfailed",this,function() {
        OpenLayers.Console.log('Location detection failed');
    });
*/
    vector.removeAllFeatures();
  

}
//setting up draw controls
var vectors;
function osm_tools_draw(){
               
    vectors = new OpenLayers.Layer.Vector("Vector Layer");
                
    map.addLayers([vectors]);
    map.addControl(new OpenLayers.Control.MousePosition());
    drawControls = {
        box: new OpenLayers.Control.DrawFeature(vectors,
            OpenLayers.Handler.RegularPolygon, {
                handlerOptions: {
                    sides: 4,
                    irregular: true
                }
            }
            )
    };
                
                
    //Return box coordinates after box is drawn
    vectors.events.on({
        'featuresadded': ReturnCoords
    });
                
    function ReturnCoords(event){
        var b = event.features[0].geometry.getBounds();
        b.transform(map.getProjectionObject(), new OpenLayers.Projection("EPSG:4326"));
        boxCoords='';
        boxCoords += b.top+","+ b.bottom+","+ b.right+","+ b.left;
        var e = document.getElementById('boxC');
        if(e) e.value = boxCoords;
    }  
                
    //Remove points before adding new points
    vectors.events.on({
        'beforefeatureadded': function(){
            vectors.removeAllFeatures()
        }
    });
                
            
    for(var key in drawControls) {
        map.addControl(drawControls[key]);
    }
    map.setCenter(new OpenLayers.LonLat(0, 0), 3);
    document.getElementById('noneToggle').checked = true;
}

//allowing toggle control of certain drawing controls

function toggleControl(element) {
    for(key in drawControls) {
        var control = drawControls[key];
        if(element.value == key && (element.checked || element.onclick )) {
            control.activate();
        } else {
            control.deactivate();
        }
    }
}



//interface tools
function doErase(field){
    var a = document.getElementById(field);
    var t = document.getElementById('tname');
    var f = document.getElementById('fname');
    a.value = '';
    if (field == 'fname'){
        delStart();
        if(t.value=='')pathlocs = new Array();
    }
        
    else if(field == 'tname'){
        delEnd();
        if(f.value =='')pathlocs = new Array();
    }
    else if(field =='upload'){
        delEnd();
        delStart();
        pathlocs = new Array();
    }
}
function delEnd(){
    if(markerend)markers.removeMarker(markerend);
    lineLayer.destroyFeatures();
    pathlocs.splice(pathlocs.length-1,1);
}
function delStart(){
    if(markerstart)markers.removeMarker(markerstart);
    lineLayer.destroyFeatures();
    map.events.unregister('click', map, handleEndClick);
}



/*
 * The main function 
 */
jQuery(function()
{
    // Initialize the map to the user location and draw controls 
    osm_tools_userLocation();
    osm_tools_draw();
});



var geocoder;
var gmaps;
function initialize() {
geocoder = new google.maps.Geocoder();
var latlng = new google.maps.LatLng(-34.397, 150.644);
var myOptions = {
  zoom: 8,
  center: latlng,
  mapTypeId: google.maps.MapTypeId.ROADMAP
}
//gmaps = new google.maps.Map(document.getElementById("map_canvas"), myOptions);
}

function codeAddress() {
initialize();
var address = document.getElementById("address").value;
geocoder.geocode( { 'address': address}, function(results, status) {
  if (status == google.maps.GeocoderStatus.OK) {
var lat =  results[0].geometry.location.lat();
var lon  = results[0].geometry.location.lng();
epsg4326 =  new OpenLayers.Projection("EPSG:4326"); //WGS 1984 projection
projectTo = map.getProjectionObject(); //The map projection (Spherical Mercator)
var lonLat = new OpenLayers.LonLat( lon , lat ).transform(epsg4326, projectTo);
var zoom=9;
map.setCenter (lonLat, zoom);   



// map.setCenter(results[0].geometry.location);
   // var marker = new google.maps.Marker({
   //     map: map, 
   //     position: results[0].geometry.location
   // });
  } else {
    alert("Geocode was not successful for the following reason: " + status);
  }
 });
 }


/**
** The following section for drawing Box from partitions. 
**
**/

//Attributes 
 var boxes  = new OpenLayers.Layer.Boxes( "partitions" );
 var box_extents = [
            [-10, 50, 5, 60],
            [-75, 41, -71, 44],
            [-122.6, 37.6, -122.3, 37.9],
            [10, 10, 20, 20]
        ];	
//function
function drawBoxPartition(){
  

           
            for (var i = 0; i < box_extents.length; i++) {
                ext = box_extents[i];
                bounds = OpenLayers.Bounds.fromArray(ext);
                box = new OpenLayers.Marker.Box(bounds);
                box.events.register("click", box, function (e) {
                    this.setBorder("black");
                });
                boxes.addMarker(box);
            }

            map.addLayers([boxes]);

	
}



function clearMap(){
/*
  var num = map.getNumLayers();
  //alert("number of layers:"+num);
  for (var j=1; j<num; j++) {
   alert(map.layers[j].name);
   if(map.layers[j].name = "partitions"){
    alert("Im in this layer: "+map.layers[j].name);
    map.removeLayer( boxes );
    //map.removeLayer( map.layers[j] );
   }
  }
*/
	map.removeLayer( boxes ); 
}

