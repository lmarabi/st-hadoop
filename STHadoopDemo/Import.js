/* 
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


function Importer() {
    pathlocs = new Array();
    var file1 = document.getElementById('Nodes').files[0];
    var file2 = document.getElementById('Edges').files[0];
    if (!file1) {
        alert("Missing node file!")
        return;
    }
    if (!file2) {
        alert("Missing edge file!")
        return;
    }
    var reader = new FileReader();
    var reader2 = new FileReader();
    var array;
    var j = 0;
    var Nodes = new Array();
    var Edges = new Array();
    reader.onload = function(e) {
        array = e.target.result.split('\n');
        var t;
        var longlat = /[0-9]+\.*[0-9]*/;
        for (var i = 1; i < array.length; i++) {
            t = array[i].split(',')
            if (array[i] == '')
                break;
            if (isNaN(t[0])
                    || !longlat.test(t[1]) || isNaN(t[1])
                    || !longlat.test(t[2]) || isNaN(t[2])) {
                alert("Error in Node file!")
                return;
            }
            var lonLat = new OpenLayers.LonLat(t[2], t[1]);
            lonLat.transform(new OpenLayers.Projection("EPSG:4326"), new OpenLayers.Projection("EPSG:900913"));
            Nodes[parseInt(t[0])] = new OpenLayers.Geometry.Point(lonLat.lon, lonLat.lat)
        }

        j = 0;
        reader2.onload = function(e) {
            array = e.target.result.split('\n');
            for (var i = 1; i < array.length; i++) {
                t = array[i].split(",");
                if (t[0] == '')
                    break;
                if (isNaN(t[0])) {
                    alert("Error in Edge file!");
                    return;
                }
                if (!isNaN(t[1])) {
                    Edges[j] = {
                        nodeIDFrom: parseInt(t[1]),
                        nodeIDTo: parseInt(t[2])
                    };
                    j++
                }
                else {
                    var pat = /[\"{}]/;
                    var s = t[1].split(pat);
                    s = s[2].split(/[, ]/);
                    var a = new Array();
                    for (var k = 0; k < s.length; k++) {
                        if (isNaN(s[k])) {
                            alert("Error in Edge file!");
                            return;
                        }
                        a[k] = s[k];
                    }
                    Edges[j] = a;
                    j++;
                }
            }

            alert("starting...")
            var toNode;
            var fromNode;
            for (j = 0; j < Edges.length; j++) {
                if (Edges[j].nodeIDFrom != null) {
                    fromNode = Nodes[Edges[j].nodeIDFrom]
                    toNode = Nodes[Edges[j].nodeIDTo]
                    path(fromNode, toNode)
                }
                for (i = 0; i < Edges[j].length - 1; i++) {
                    fromNode = Nodes[Edges[j][i]]
                    toNode = Nodes[Edges[j][i + 1]]
                    path(fromNode, toNode)
                }
            }
            alert("Finished!");

        }
        reader2.readAsText(file2);
    };
    reader.readAsText(file1);
}


function Import(nodes, edges) {
    pathlocs = new Array();
    var array;
    var j = 0;
    var Nodes = new Array();
    var Edges = new Array();
    array = nodes.split(';');
    var t;
    var longlat = /[0-9]+\.*[0-9]*/;
    for (var i = 0; i < array.length; i++) {
        t = array[i].split(',')
        if (t == '')
            break;
        if (isNaN(t[0])
                || !longlat.test(t[1]) || isNaN(t[1])
                || !longlat.test(t[2]) || isNaN(t[2])) {
            alert("Error in Node file!"+t[0]+t[1]+t[2])
            return;
        }
        var lonLat = new OpenLayers.LonLat(t[2], t[1]);
        lonLat.transform(new OpenLayers.Projection("EPSG:4326"), new OpenLayers.Projection("EPSG:900913"));
        Nodes[parseInt(t[0])] = new OpenLayers.Geometry.Point(lonLat.lon, lonLat.lat)
    }

    j = 0;
    array = edges.split(';');
    for (var i = 0; i < array.length; i++) {
        t = array[i].split(',');
        if (t[0] == '')
            break;
        if (isNaN(t[0])) {
            alert("Error in Edge file!");
            return;
        }
        if (!isNaN(t[1])) {
            Edges[j] = {
                nodeIDFrom: parseInt(t[1]),
                nodeIDTo: parseInt(t[2])
            };
            j++
        }
        else {
            var pat = /[\"{}]/;
            var s = t[1].split(pat);
            s = s[2].split(',');
            var a = new Array();
            for (var k = 0; k < s.length; k++) {
                if (isNaN(s[k])) {
                    alert("Error in Edge file");
                    return;
                }
                a[k] = s[k];
            }
            Edges[j] = a;
            j++;
        }
    }
   alert("start drawing ");
    var toNode;
    var fromNode;
    for (j = 0; j < Edges.length; j++) {
        if (Edges[j].nodeIDFrom != null) {
            fromNode = Nodes[Edges[j].nodeIDFrom];
            toNode = Nodes[Edges[j].nodeIDTo];
            path(fromNode, toNode);
        }
        for (i = 0; i < Edges[j].length - 1; i++) {
            fromNode = Nodes[Edges[j][i]];
            toNode = Nodes[Edges[j][i + 1]];
            path(fromNode, toNode);
        }
    }
   alert("finish");
}


