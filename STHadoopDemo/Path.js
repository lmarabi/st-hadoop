/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


//enter starting point of the path
var handleStartClick;
var handleEndClick;
var startTrig=0;
var endTrig=0;

function getStart(){
    
    handleStartClick = function (e)
    {
        markers.removeMarker(markerstart);
        lineLayer.destroyFeatures();
        var lonlats = map.getLonLatFromViewPortPx(e.xy);
        var lonLat = new OpenLayers.LonLat(lonlats.lon, lonlats.lat);    
        var src = new OpenLayers.Marker(lonLat);
        markers.addMarker(src);
        markerstart = src;
        if(index>0)pathlocs[0] = new OpenLayers.Geometry.Point(lonlats.lon,lonlats.lat);
        else{
            pathlocs[index] = new OpenLayers.Geometry.Point(lonlats.lon,lonlats.lat);
            index++;
        }
        
        lonlats.transform(
            new OpenLayers.Projection("EPSG:900913"),
            new OpenLayers.Projection("EPSG:4326")
            );
        document.getElementById('fname').value = lonlats.lon + ","+ lonlats.lat;
    }
    map.events.register('click', map, handleStartClick);
    startTrig=1;
}


function getEnd(){
    if(startTrig==1){
        map.events.unregister('click', map, handleStartClick);
        startTrig=0;
    }
    handleEndClick = function(e)
    {  
        if(markerend){
            markers.removeMarker(markerend);
            index--;
            lineLayer.destroyFeatures();
        } 
        var lonlate = map.getLonLatFromViewPortPx(e.xy);
        var lonLat = new OpenLayers.LonLat(lonlate.lon, lonlate.lat);    
        var dest = new OpenLayers.Marker(lonLat)
        markers.addMarker(dest);
        markerend = dest;
        pathlocs[index] = new OpenLayers.Geometry.Point(lonlate.lon,lonlate.lat);
        index++;
        lonlate.transform(
            new OpenLayers.Projection("EPSG:900913"),
            new OpenLayers.Projection("EPSG:4326")
            );
        document.getElementById('tname').value = lonlate.lon + ","+ lonlate.lat;
    }
    map.events.register('click', map, handleEndClick);
    endTrig=1;
    
}

// start drawing the path

function path(from, to){
    var p = new Array()
        p.push(from)
        p.push(to)
    var line = new OpenLayers.Geometry.LineString(p);
    var style = { 
        strokeColor: 'blue', 
        strokeOpacity: 0.5,
        strokeWidth: 5
    };
    var lineFeature = new OpenLayers.Feature.Vector(line, null, style); 
    lineLayer.addFeatures([lineFeature]);
}
